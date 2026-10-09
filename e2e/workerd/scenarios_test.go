package workerd

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// What an application in Cloudflare sees of the tunnel: fetch, connect,
// several controllers, and the object restarting.

func TestFetch(t *testing.T) {
	s := newStand(t)
	s.addController()

	t.Run("get: query kept, fragment dropped, credential injected", func(t *testing.T) {
		out := s.run("get")
		want := map[string]any{"uri": "/a/b?x=1", "x-test": "yes", "authorization": "Bearer injected", "who": "upstream"}
		for k, v := range want {
			if got := get(out, "body", k); got != v {
				t.Errorf("%s = %v, want %v", k, got, v)
			}
		}
	})

	t.Run("https: re-issued over TLS by the controller", func(t *testing.T) {
		out := s.run("https")
		if get(out, "body", "tls") != true || get(out, "body", "authorization") != "Bearer injected-tls" {
			t.Errorf("got %v", out)
		}
	})

	t.Run("post with a known length", func(t *testing.T) {
		out := s.run("post")
		if get(out, "body", "bodySha256") != get(out, "sha256") || get(out, "body", "contentLength") != float64(300000) {
			t.Errorf("got %v", out)
		}
	})

	t.Run("streamed body goes chunked", func(t *testing.T) {
		out := s.run("stream")
		if get(out, "body", "bodyLength") != float64(40*128*1024) {
			t.Errorf("got %v", out)
		}
	})

	// 64 MiB with no declared length reaches the object over RPC as 16,384
	// pieces of 4 KiB — more than workerd's cap of 10,000 live timers, which
	// one timer left behind per piece would hit.
	t.Run("64 MiB upload of unknown length", func(t *testing.T) {
		if out := s.run("upload", fmt.Sprintf("n=%d", 64<<20)); out["total"] != float64(64<<20) {
			t.Errorf("got %v", out)
		}
	})

	t.Run("20 MiB response", func(t *testing.T) {
		out := s.run("big")
		if out["total"] != float64(20*1024*1024) {
			t.Errorf("got %v", out)
		}
	})

	t.Run("sse arrives event by event", func(t *testing.T) {
		out := s.run("sse")
		arrivals, _ := out["arrivals"].([]any)
		if len(arrivals) < 3 {
			t.Fatalf("got %v", out)
		}
		first := arrivals[0].([]any)[0].(float64)
		last := arrivals[len(arrivals)-1].([]any)[0].(float64)
		if first > 150 || last < 300 {
			t.Errorf("events not spread out: %v", arrivals)
		}
	})

	t.Run("statuses", func(t *testing.T) {
		out := s.run("statuses")
		for code, body := range map[string]string{"204": "", "304": "", "404": "status body", "500": "status body"} {
			if get(out, code, "body") != body || get(out, code, "status") != mustFloat(code) {
				t.Errorf("%s: got %v", code, out[code])
			}
		}
	})

	t.Run("repeated set-cookie", func(t *testing.T) {
		out := s.run("cookies")
		if fmt.Sprint(out["cookies"]) != "[a=1 b=2]" {
			t.Errorf("got %v", out)
		}
	})

	t.Run("no route: 502 from the object, nothing reaches the internet", func(t *testing.T) {
		out := s.run("noroute")
		if out["status"] != float64(502) || out["error"] != "no-route" {
			t.Errorf("got %v", out)
		}
	})

	t.Run("50 at once", func(t *testing.T) {
		out := s.run("parallel")
		if out["ok"] != float64(50) {
			t.Errorf("got %v", out)
		}
	})
}

func TestConnect(t *testing.T) {
	s := newStand(t)
	s.addController()

	t.Run("raw tcp, server first, half-close answered", func(t *testing.T) {
		if out := s.run("tcp"); out["got"] != "HELLO\nping\nBYE\n" {
			t.Errorf("got %v", out)
		}
	})
	t.Run("no route: the socket ends empty", func(t *testing.T) {
		if out := s.run("tcpnoroute"); out["got"] != "" || out["error"] != nil {
			t.Errorf("got %v", out)
		}
	})
	t.Run("secureTransport on is refused by the runtime", func(t *testing.T) {
		if out := s.run("tcptls"); !strings.Contains(fmt.Sprint(out["error"]), "Incoming CONNECT with TLS not supported") {
			t.Errorf("got %v", out)
		}
	})
}

// Several controller pods on one object: calls spread across them, and one
// dying takes nothing else down.
func TestControllers(t *testing.T) {
	s := newStand(t)
	for range 3 {
		s.addController()
	}

	for range 3 {
		if out := s.run("parallel", "n=20"); out["ok"] != float64(20) {
			t.Fatalf("before the kill: %v", out)
		}
	}

	// A pod killed while calls are in flight. Those on it fail as network
	// errors — the application's fetch rejects — and nothing else does.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, failed int
	for range 10 {
		wg.Go(func() {
			out := s.tryRun("parallel", "n=20")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case out["ok"] == float64(20):
				ok++
			case isNetworkError(fmt.Sprint(out["threw"])):
				failed++
			default:
				t.Errorf("not a network error: %v", out)
			}
		})
	}
	time.Sleep(30 * time.Millisecond)
	s.ctrls[0].Process.Kill()
	wg.Wait()
	t.Logf("during the kill: %d runs fine, %d hit the dead pod", ok, failed)

	s.waitFor("the dead controller to be dropped", func() bool { return s.status().Controllers == 2 })
	for range 5 {
		if out := s.run("parallel", "n=20"); out["ok"] != float64(20) {
			t.Fatalf("after the kill: %v", out)
		}
	}
}

// workerd restarting is what eviction looks like from outside: every socket
// closes with the object. Controllers reconnect and declare their routes
// again, and the application works as before.
func TestObjectRestart(t *testing.T) {
	s := newStand(t)
	s.addController()
	s.addController()
	if out := s.run("get"); get(out, "status") != float64(200) {
		t.Fatalf("before: %v", out)
	}

	s.workerd.Process.Kill()
	s.workerd.Wait()
	s.startWorkerd()

	s.waitFor("controllers to reconnect", func() bool {
		st := s.status()
		return st.Controllers == 2 && st.Routes
	})
	if out := s.run("get"); get(out, "status") != float64(200) {
		t.Fatalf("after: %v", out)
	}
}

func mustFloat(s string) float64 {
	var f float64
	fmt.Sscan(s, &f)
	return f
}

// A controller dying mid-body must fail the application's read, not end it:
// a truncated response that reads as complete is the worst way to fail. The
// gateway enforces the declared length (FixedLengthStream), which is what
// makes workerd report it.
func TestTruncatedBodyFails(t *testing.T) {
	s := newStand(t)
	s.addController()
	const n = 500 << 20

	done := make(chan map[string]any, 1)
	go func() { done <- s.tryRun("cut", fmt.Sprintf("n=%d", n)) }()
	time.Sleep(150 * time.Millisecond)
	s.ctrls[0].Process.Kill()

	out := <-done
	total, _ := out["total"].(float64)
	if total >= n {
		t.Skipf("the download finished before the kill (%v); nothing to check", out)
	}
	if out["error"] == nil {
		t.Fatalf("cut off at %v of %d bytes, and the application was told the body was complete", total, n)
	}
	t.Logf("cut off at %.0f of %d bytes: %v", total, n, out["error"])
}

// isNetworkError reports whether an application's error says the connection
// was lost — the only acceptable way for a call on a dying controller to fail.
// A truncated body read as complete ("Unexpected end of JSON input") is not.
func isNetworkError(e string) bool {
	for _, s := range []string{
		"YamuxError: ",             // the session went: WebSocket error, transport closed, session shutdown
		"Network connection lost",  // a FixedLengthStream fell short
		"disconnected prematurely", // a body errored across RPC
	} {
		if strings.Contains(e, s) {
			return true
		}
	}
	return false
}
