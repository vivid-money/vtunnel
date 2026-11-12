package session

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
)

// writes records every Write that reaches the connection under yamux. With no
// Conn it only records.
type writes struct {
	net.Conn
	mu  sync.Mutex
	got [][]byte
}

func (w *writes) Write(b []byte) (int, error) {
	w.mu.Lock()
	w.got = append(w.got, bytes.Clone(b))
	w.mu.Unlock()
	if w.Conn == nil {
		return len(b), nil
	}
	return w.Conn.Write(b)
}

func (w *writes) all() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.got...)
}

// header builds a yamux frame header (hashicorp/yamux const.go).
func header(typ byte, flags uint16, id, length uint32) []byte {
	h := make([]byte, frameHeaderSize)
	h[1] = typ
	binary.BigEndian.PutUint16(h[2:], flags)
	binary.BigEndian.PutUint32(h[4:], id)
	binary.BigEndian.PutUint32(h[8:], length)
	return h
}

const (
	typeData         = 0
	typeWindowUpdate = 1
	typePing         = 2
	typeGoAway       = 3
)

// Under yamux-wss one Write is one WebSocket message: a data frame must reach
// the connection whole, header and body in one call.
func TestDataFrameIsOneWrite(t *testing.T) {
	a, b := net.Pipe()
	rec := &writes{Conn: a}

	// yamux.Server does no I/O, so nothing has to be reading a yet.
	srv, err := serveYamux(b, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	client, err := dialYamux(rec, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })

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
		if len(w) > frameHeaderSize && w[1] == typeData {
			whole[len(w)-frameHeaderSize] = true
		}
	}
	for _, p := range payloads {
		if !whole[len(p)] {
			t.Errorf("no single write carried the %d-byte frame with its header", len(p))
		}
	}
}

// frameConn joins a data header to the body that follows it and to nothing
// else, and holds nothing else back: a window update or a ping left waiting
// for the next Write could wait for ever.
func TestFrameConnWrites(t *testing.T) {
	// A 12-byte body that is itself a valid data header: it must be taken as
	// the held header's body, not held as a header of its own.
	lookalike := header(typeData, 0, 7, 9)

	steps := []struct {
		write []byte
		out   [][]byte // what reaches the connection on this Write
	}{
		{header(typeWindowUpdate, 1, 1, 0), [][]byte{header(typeWindowUpdate, 1, 1, 0)}},
		{header(typeData, 0, 1, 5), nil},
		{[]byte("hello"), [][]byte{append(header(typeData, 0, 1, 5), "hello"...)}},
		{header(typePing, 1, 0, 42), [][]byte{header(typePing, 1, 0, 42)}},
		{header(typeData, 0, 1, 12), nil},
		{lookalike, [][]byte{append(header(typeData, 0, 1, 12), lookalike...)}},
		{header(typeData, 0, 1, 0), [][]byte{header(typeData, 0, 1, 0)}},
		{header(typeGoAway, 0, 0, 0), [][]byte{header(typeGoAway, 0, 0, 0)}},
	}

	rec := &writes{}
	c := &frameConn{Conn: rec}
	seen := 0
	for i, s := range steps {
		n, err := c.Write(s.write)
		if n != len(s.write) || err != nil {
			t.Fatalf("step %d: Write(%d bytes) = %d, %v", i, len(s.write), n, err)
		}
		got := rec.all()[seen:]
		seen += len(got)
		if len(got) != len(s.out) {
			t.Fatalf("step %d: %d writes reached the connection, want %d", i, len(got), len(s.out))
		}
		for j := range got {
			if !bytes.Equal(got[j], s.out[j]) {
				t.Fatalf("step %d: wrote % x, want % x", i, got[j], s.out[j])
			}
		}
	}
}

// A frame as big as the stream window does not stay with the session.
func TestFrameConnDropsLargeFrame(t *testing.T) {
	c := &frameConn{Conn: &writes{}}
	body := make([]byte, 8<<20)
	c.Write(header(typeData, 0, 1, uint32(len(body))))
	c.Write(body)
	if cap(c.frame) > maxKeptFrame {
		t.Fatalf("kept a %d-byte frame buffer", cap(c.frame))
	}
}
