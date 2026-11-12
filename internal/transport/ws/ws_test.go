package ws

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// pair dials a server whose accepted connection is handed to serve.
func pair(t *testing.T, serve func(*websocket.Conn)) net.Conn {
	t.Helper()
	up := Upgrader(time.Second)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serve(c)
	}))
	t.Cleanup(ts.Close)
	conn, err := Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http"), nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// A peer that answers the heartbeat keeps the connection up, and the
// heartbeat never shows up in the bytes either side reads.
func TestHeartbeatAnsweredAndInvisible(t *testing.T) {
	conn := pair(t, func(raw *websocket.Conn) {
		peer := Conn(raw)
		io.Copy(peer, peer) // echoes data; answers pings inside Read
	})
	Heartbeat(conn, 10*time.Millisecond, 50*time.Millisecond)

	deadline := time.Now().Add(300 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		msg := []byte{byte(i), byte(i >> 8)}
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		got := make([]byte, 2)
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if got[0] != msg[0] || got[1] != msg[1] {
			t.Fatalf("read %v, want %v: heartbeat text leaked into the stream", got, msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A peer that stops answering — the link gone half-open — gets the
// connection closed within the timeout.
func TestHeartbeatClosesASilentLink(t *testing.T) {
	conn := pair(t, func(raw *websocket.Conn) {
		for { // reads everything, answers nothing
			if _, _, err := raw.NextReader(); err != nil {
				return
			}
		}
	})
	Heartbeat(conn, 10*time.Millisecond, 50*time.Millisecond)

	done := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("read succeeded on a silent link")
		}
	case <-time.After(time.Second):
		t.Fatal("a silent link was not closed")
	}
}

// peerClose serves a peer that reads until the connection ends and hands back
// how it ended: the close frame, if one came.
func peerClose(t *testing.T) (net.Conn, <-chan error) {
	t.Helper()
	ended := make(chan error, 1)
	conn := pair(t, func(raw *websocket.Conn) {
		for {
			if _, _, err := raw.NextReader(); err != nil {
				ended <- err
				return
			}
		}
	})
	return conn, ended
}

func closeFrame(t *testing.T, ended <-chan error) *websocket.CloseError {
	t.Helper()
	select {
	case err := <-ended:
		var ce *websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("the peer saw %v, not a close frame", err)
		}
		return ce
	case <-time.After(time.Second):
		t.Fatal("the peer never saw the connection end")
		return nil
	}
}

// Closing says so: the peer gets a close frame, not just the end of the TCP
// connection, which through a proxy may never reach it.
func TestCloseSendsACloseFrame(t *testing.T) {
	conn, ended := peerClose(t)
	conn.Close()
	if ce := closeFrame(t, ended); ce.Code != websocket.CloseNormalClosure {
		t.Errorf("close code %d, want %d", ce.Code, websocket.CloseNormalClosure)
	}
}

// captureLog collects what is logged until the test ends.
func captureLog(t *testing.T) func() string {
	t.Helper()
	var logs strings.Builder
	var mu sync.Mutex
	prev := log.Writer()
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return logs.Write(p)
	}))
	t.Cleanup(func() { log.SetOutput(prev) })
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		return logs.String()
	}
}

// readToEnd reads conn until it fails.
func readToEnd(conn net.Conn) error {
	buf := make([]byte, 4096)
	for {
		if _, err := conn.Read(buf); err != nil {
			return err
		}
	}
}

// A peer that closes for a reason is logged with it; a plain goodbye is not.
func TestPeerCloseIsLoggedWithItsReason(t *testing.T) {
	logs := captureLog(t)
	closing := func(code int, reason string) net.Conn {
		return pair(t, func(raw *websocket.Conn) {
			raw.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
			time.Sleep(100 * time.Millisecond)
		})
	}
	readToEnd(closing(websocket.CloseNormalClosure, ""))
	if got := logs(); got != "" {
		t.Errorf("a plain goodbye was logged: %q", got)
	}
	readToEnd(closing(websocket.CloseGoingAway, "vtunnel: controller stopped answering"))
	if got := logs(); !strings.Contains(got, "controller stopped answering") {
		t.Errorf("the peer's reason was not logged: %q", got)
	}
}

// A connection that drops is logged wherever the reader was: between
// messages, or part of the way through one.
func TestDropIsLoggedMidMessageToo(t *testing.T) {
	for name, send := range map[string]func(*websocket.Conn){
		"between messages": func(raw *websocket.Conn) {
			raw.WriteMessage(websocket.BinaryMessage, make([]byte, 100))
		},
		"mid-message": func(raw *websocket.Conn) {
			w, err := raw.NextWriter(websocket.BinaryMessage)
			if err != nil {
				return
			}
			w.Write(make([]byte, 3*BufferSize)) // goes out as fragments of a message that never ends
		},
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureLog(t)
			conn := pair(t, func(raw *websocket.Conn) {
				send(raw)
				time.Sleep(50 * time.Millisecond)
				raw.NetConn().Close()
			})
			if err := readToEnd(conn); err == nil {
				t.Fatal("read succeeded on a dropped connection")
			}
			if got := logs(); strings.Count(got, "connection lost") != 1 {
				t.Errorf("want the drop logged once, got %q", got)
			}
		})
	}
}

// Giving up on a silent link tells the peer why, and logs it here.
func TestHeartbeatSaysWhyItClosed(t *testing.T) {
	logs := captureLog(t)

	conn, ended := peerClose(t)
	Heartbeat(conn, 10*time.Millisecond, 50*time.Millisecond)

	ce := closeFrame(t, ended)
	if ce.Code != websocket.CloseGoingAway || !strings.Contains(ce.Text, "no pong") {
		t.Errorf("close frame %d %q, want %d saying no pong", ce.Code, ce.Text, websocket.CloseGoingAway)
	}
	if got := logs(); !strings.Contains(got, "no pong") {
		t.Errorf("nothing logged about the missing pong: %q", got)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
