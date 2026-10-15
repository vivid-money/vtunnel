package workerd

import (
	"encoding/json"
	"strings"
	"syscall"
	"testing"
	"time"
)

func (s *stand) incarnation() string {
	res, err := https.Get("https://" + s.addr() + "/status")
	if err != nil {
		s.t.Fatal(err)
	}
	defer res.Body.Close()
	var v struct{ Incarnation string }
	json.NewDecoder(res.Body).Decode(&v)
	return v.Incarnation
}

// sleepUntilEvicted leaves the object alone until workerd evicts it — about
// ten seconds idle — and reports the incarnation it wakes as.
func (s *stand) sleepUntilEvicted(before string) string {
	s.t.Helper()
	for range 4 {
		time.Sleep(12 * time.Second)
		if now := s.incarnation(); now != before {
			return now
		}
	}
	s.t.Fatal("the object was never evicted")
	return ""
}

// The object hibernates while idle and wakes with its controllers still
// connected: their sessions and routes are rebuilt from the sockets'
// attachments, and nobody reconnects. Several cycles, so that stream IDs are
// carried past every wake.
func TestHibernation(t *testing.T) {
	s := newStand(t)
	s.addController()
	s.addController()
	incarnation := s.incarnation()

	for cycle := range 3 {
		incarnation = s.sleepUntilEvicted(incarnation)

		if st := s.status(); st.Controllers != 2 || !st.Routes {
			t.Fatalf("cycle %d: woke with %+v", cycle, st)
		}
		if out := s.run("get"); get(out, "status") != float64(200) || get(out, "body", "authorization") != "Bearer injected" {
			t.Fatalf("cycle %d: get: %v", cycle, out)
		}
		if out := s.run("tcp"); out["got"] != "HELLO\nping\nBYE\n" {
			t.Fatalf("cycle %d: tcp: %v", cycle, out)
		}
		if out := s.run("parallel"); out["ok"] != float64(50) {
			t.Fatalf("cycle %d: parallel: %v", cycle, out)
		}
	}

	for i, logs := range s.ctrlLogs {
		if strings.Contains(logs.String(), "Reconnect") {
			t.Errorf("controller %d reconnected:\n%s", i, logs)
		}
	}
}

// A controller that stops answering without closing its connection — frozen,
// its host gone, a half-open link — is found out by its silence, and calls go
// to the one still alive.
func TestSilentControllerIsDropped(t *testing.T) {
	s := newStand(t)
	s.addController()
	s.addController()
	s.ctrls[0].Process.Signal(syscall.SIGSTOP)
	defer s.ctrls[0].Process.Signal(syscall.SIGCONT)

	time.Sleep(5 * time.Second) // past the stand's staleMs of 4 s
	for range 3 {
		if out := s.run("parallel", "n=20"); out["ok"] != float64(20) {
			t.Fatalf("with one controller frozen: %v", out)
		}
	}
	if st := s.status(); st.Controllers != 1 {
		t.Errorf("controllers = %d, want the frozen one dropped", st.Controllers)
	}
}
