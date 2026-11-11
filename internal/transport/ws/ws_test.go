package ws

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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

// Giving up on a silent link tells the peer why, and logs it here.
func TestHeartbeatSaysWhyItClosed(t *testing.T) {
	var logs strings.Builder
	var mu sync.Mutex
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return logs.Write(p)
	}))
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	conn, ended := peerClose(t)
	Heartbeat(conn, 10*time.Millisecond, 50*time.Millisecond)

	ce := closeFrame(t, ended)
	if ce.Code != websocket.CloseGoingAway || !strings.Contains(ce.Text, "no pong") {
		t.Errorf("close frame %d %q, want %d saying no pong", ce.Code, ce.Text, websocket.CloseGoingAway)
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(logs.String(), "no pong") {
		t.Errorf("nothing logged about the missing pong: %q", logs.String())
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
