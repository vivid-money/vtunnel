package session

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/hashicorp/yamux"
)

// defaultMuxWindow is the per-stream receive window when Config says nothing.
//
// It is not yamux's own default, and not gost's — both leave it at 256 KB.
// Taking that would ship a backend slower than the one it exists to beat: a
// stream cannot exceed window/RTT, and SSH's hard-coded 2 MB would win by
// almost an order of magnitude on any link with latency. This is the smallest
// round number that is comfortably ahead of it instead.
//
// The figure is a ceiling on what may be in flight, not memory reserved: yamux
// grows recvBuf only while the local reader falls behind the tunnel, and the
// tunnel's reader is a copy loop that does not. The worst case is real, though
// — a stalled target can hold this much per connection — so it is a setting:
// Config.StreamWindow, reachable as WithStreamWindow and
// WithServerStreamWindow.
const defaultMuxWindow = 8 * 1024 * 1024

// conn arrives already authenticated and encrypted — see secureClient and
// secureServer. yamux itself brings no cryptography, and nothing in this file
// makes up for that.

func muxConfig(cfg Config) *yamux.Config {
	c := yamux.DefaultConfig()
	// Keepalive is the tunnel layer's job, done the same way on every backend
	// rather than once per multiplexer that happens to offer it.
	c.EnableKeepAlive = false
	c.MaxStreamWindowSize = defaultMuxWindow
	if cfg.StreamWindow > 0 {
		c.MaxStreamWindowSize = uint32(cfg.StreamWindow)
	}
	c.LogOutput = io.Discard
	return c
}

func dialYamux(conn net.Conn, cfg Config) (Session, error) {
	s, err := yamux.Client(&frameConn{Conn: conn}, muxConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("yamux: %w", err)
	}
	return muxSession{s}, nil
}

func serveYamux(conn net.Conn, cfg Config) (Session, error) {
	s, err := yamux.Server(&frameConn{Conn: conn}, muxConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("yamux: %w", err)
	}
	return muxSession{s}, nil
}

// frameHeaderSize is yamux's frame header: version, type, flags, stream id,
// length (hashicorp/yamux const.go).
const frameHeaderSize = 12

// frameConn hands each yamux data frame to conn in one Write.
//
// yamux's send loop writes a data frame as two calls: the 12-byte header, then
// the body (hashicorp/yamux session.go, sendLoop). Under yamux-wss every Write
// is one WebSocket message, so every frame was two — and on Cloudflare each
// message a Durable Object receives wakes it, is billed and is logged on its
// own. Under KindYamux it was two TLS records.
//
// frameConn holds a data header that announces a body and sends it together
// with the next Write, which is that body: the send loop is conn's only writer
// and writes the body right after the header, so nothing waits. The bytes and
// their order are unchanged; only where one message ends moves.
//
// frame is reused from one frame to the next, as the send loop reuses its own
// body buffer: it grows to the largest frame and stays there.
type frameConn struct {
	net.Conn
	frame []byte
	held  bool
}

func (c *frameConn) Write(b []byte) (int, error) {
	if !c.held && announcesBody(b) {
		c.frame = append(c.frame[:0], b...)
		c.held = true
		return len(b), nil
	}
	if !c.held {
		return c.Conn.Write(b)
	}
	c.held = false
	c.frame = append(c.frame, b...)
	n, err := c.Conn.Write(c.frame)
	return max(n-frameHeaderSize, 0), err
}

// announcesBody says b is exactly a yamux data header with a non-zero length.
// Window updates, pings and go-aways carry no body; a data frame of length 0
// (a bare SYN, ACK or FIN) has none either.
func announcesBody(b []byte) bool {
	const typeData = 0
	return len(b) == frameHeaderSize && b[0] == 0 && b[1] == typeData && binary.BigEndian.Uint32(b[8:12]) > 0
}

// muxSession is yamux with the one method it is missing, and streams that can
// say "I am done writing" without also saying "I am done".
type muxSession struct{ *yamux.Session }

func (m muxSession) Wait() error {
	<-m.CloseChan()
	return io.EOF
}

func (m muxSession) Open() (net.Conn, error) {
	stream, err := m.Session.OpenStream()
	if err != nil {
		return nil, err
	}
	return muxStream{stream}, nil
}

func (m muxSession) Accept() (net.Conn, error) {
	stream, err := m.Session.AcceptStream()
	if err != nil {
		return nil, err
	}
	return muxStream{stream}, nil
}

// muxStream gives a yamux stream the CloseWrite every other stream in this
// package has.
//
// A forwarded connection is half-closable at both ends — a TCP socket on one
// side, an SSH channel or this on the other — and the tunnel is the only thing
// in the middle. Whoever cannot pass a FIN along as a FIN turns "I have
// finished asking" into "I have finished", which kills the answer while it is
// still being written. Every request/response protocol that frames its request
// by end-of-stream depends on this.
type muxStream struct{ *yamux.Stream }

// CloseWrite sends FIN and nothing else. yamux spells that Close: the stream
// moves to streamLocalClose, which prohibits further local writes and — this is
// the part that matters — goes on delivering everything the peer sends
// (hashicorp/yamux stream.go, Read's streamLocalClose case). Reaching for
// Stream.Shutdown or a full teardown here would drop the reply.
func (s muxStream) CloseWrite() error { return s.Stream.Close() }
