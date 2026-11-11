package workerd

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// A controller whose link goes dead without a close reaching the object — a
// half-open connection: the controller gives up on it, its close lost on the
// way — reconnects, and its old session must leave the object at once. On
// Cloudflare the object kept the dead session beside the new one until it had
// been silent for staleMs, and calls it was given failed with "controller
// stopped answering", ran into "no response within 60000 ms", or hung.
func TestReconnectReplacesItsDeadSession(t *testing.T) {
	s := newStand(t)
	link := newSilencer(t, s.addr())
	s.addControllerAt(link.url())
	s.addController()

	link.silence()
	s.waitForLog(0, "Reconnected", 15*time.Second)

	// Before any call: the object has not had to go looking for a live
	// session, so only the reconnect can have taken the dead one away.
	if st := s.status(); st.Controllers != 2 {
		t.Fatalf("controllers = %d right after the reconnect, want 2: the dead session is still there", st.Controllers)
	}
	for range 3 {
		if out := s.run("parallel", "n=20"); out["ok"] != float64(20) {
			t.Fatalf("after the reconnect: %v", out)
		}
	}
}

// A controller that gives up on a silent link says why.
func TestControllerLogsWhyItReconnected(t *testing.T) {
	s := newStand(t)
	link := newSilencer(t, s.addr())
	s.addControllerAt(link.url())

	link.silence()
	s.waitForLog(0, "Reconnected", 15*time.Second)
	if logs := s.ctrlLogs[0].String(); !strings.Contains(logs, "no pong") {
		t.Errorf("the controller reconnected without saying why:\n%s", logs)
	}
}

func (s *stand) waitForLog(i int, text string, within time.Duration) {
	s.t.Helper()
	deadline := time.Now().Add(within)
	for !strings.Contains(s.ctrlLogs[i].String(), text) {
		if time.Now().After(deadline) {
			s.t.Fatalf("controller %d never logged %q:\n%s", i, text, s.ctrlLogs[i])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// silencer is a TCP relay in front of the stand that can go quiet: silence
// stops it carrying anything on the connections it has, in either direction,
// without closing any of them, so neither end hears the other leave. New
// connections are carried as before.
type silencer struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	conns  []net.Conn
	silent chan struct{}
}

func newSilencer(t *testing.T, target string) *silencer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &silencer{ln: ln, target: target, silent: make(chan struct{})}
	t.Cleanup(p.close)
	go p.serve()
	return p
}

func (p *silencer) url() string { return fmt.Sprintf("wss://%s/tunnel", p.ln.Addr()) }

func (p *silencer) serve() {
	for {
		in, err := p.ln.Accept()
		if err != nil {
			return
		}
		out, err := net.Dial("tcp", p.target)
		if err != nil {
			in.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, in, out)
		silent := p.silent
		p.mu.Unlock()
		go p.carry(out, in, silent)
		go p.carry(in, out, silent)
	}
}

// carry copies until silent closes, then holds both ends open, saying nothing.
func (p *silencer) carry(dst io.Writer, src io.Reader, silent chan struct{}) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		select {
		case <-silent:
			return
		default:
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// silence quiets every connection carried so far; later ones are not affected.
func (p *silencer) silence() {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.silent)
	p.silent = make(chan struct{})
}

func (p *silencer) close() {
	p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}
