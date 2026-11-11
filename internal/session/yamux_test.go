package session

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
)

// writes records every Write that reaches the connection under yamux.
type writes struct {
	net.Conn
	mu  sync.Mutex
	got [][]byte
}

func (w *writes) Write(b []byte) (int, error) {
	w.mu.Lock()
	w.got = append(w.got, bytes.Clone(b))
	w.mu.Unlock()
	return w.Conn.Write(b)
}

func (w *writes) all() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.got...)
}

// Under yamux-wss one Write is one WebSocket message: a data frame must reach
// the connection whole, header and body in one call.
func TestDataFrameIsOneWrite(t *testing.T) {
	a, b := net.Pipe()
	rec := &writes{Conn: a}

	server := make(chan Session, 1)
	go func() {
		s, err := serveYamux(b, Config{})
		if err != nil {
			t.Error(err)
		}
		server <- s
	}()
	client, err := dialYamux(rec, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	srv := <-server
	t.Cleanup(func() { srv.Close() })

	payloads := [][]byte{[]byte("hello"), bytes.Repeat([]byte("x"), 70_000)}
	stream, err := client.Open()
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := srv.Accept()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range payloads {
		if _, err := stream.Write(p); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(p))
		if _, err := io.ReadFull(accepted, got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, p) {
			t.Fatalf("read %d bytes that differ from what was written", len(got))
		}
	}

	whole := map[int]bool{}
	for _, w := range rec.all() {
		if announcesBody(w) {
			t.Fatalf("a data header went out alone: % x", w)
		}
		if len(w) > frameHeaderSize && w[1] == 0 {
			whole[len(w)-frameHeaderSize] = true
		}
	}
	for _, p := range payloads {
		if !whole[len(p)] {
			t.Errorf("no single write carried the %d-byte frame with its header", len(p))
		}
	}
}
