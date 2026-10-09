package ws

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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
