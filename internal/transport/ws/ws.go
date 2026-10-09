// Package ws carries a tunnel over a WebSocket.
//
// It is a transport and nothing more: it turns a WebSocket into an ordered
// stream of bytes and hands it over. Everything that makes the tunnel safe
// happens above it, in package session, which is why running over ws:// and
// running over wss:// come to the same thing.
//
// The accepting half is a net.Listener like any other, even though a
// WebSocket server is an HTTP server underneath. [Listen] runs that server and
// hands upgraded connections out of Accept, so the tunnel above never learns
// the difference. A deployment that needs the tunnel mounted on its own mux —
// next to a health endpoint, say — uses [Upgrader] and [Conn] directly instead.
package ws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// BufferSize is the read and write buffer gorilla uses per connection.
//
// The default is 4 KiB, which is smaller than a single session packet: every
// packet would then be flushed to the socket in several pieces. Sizing the
// buffers to match the 32 KiB copy buffer keeps one packet to one write. There
// is exactly one WebSocket connection per tunnel, so this costs 64 KiB.
const BufferSize = 32 * 1024

// Upgrader returns a websocket.Upgrader configured for tunnel traffic.
// Origin checks are left open on purpose: the tunnel authenticates itself in
// the session layer, and browser-origin rules mean nothing to it.
func Upgrader(handshakeTimeout time.Duration) websocket.Upgrader {
	return websocket.Upgrader{
		HandshakeTimeout: handshakeTimeout,
		ReadBufferSize:   BufferSize,
		WriteBufferSize:  BufferSize,
		CheckOrigin:      func(*http.Request) bool { return true },
	}
}

// Dial opens a WebSocket to url and presents it as a net.Conn.
func Dial(ctx context.Context, url string, headers http.Header, handshakeTimeout time.Duration) (net.Conn, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: handshakeTimeout,
		ReadBufferSize:   BufferSize,
		WriteBufferSize:  BufferSize,
		// The controlplane is someone's laptop, which may only reach the
		// sandbox through a corporate proxy. gorilla's own DefaultDialer does
		// this; a zero Dialer would silently ignore the environment.
		Proxy: http.ProxyFromEnvironment,
	}
	wsConn, _, err := dialer.DialContext(ctx, url, headers)
	if err != nil {
		return nil, err
	}
	return Conn(wsConn), nil
}

// DialStrict opens a wss:// WebSocket for a protocol whose security rests on
// it: the server certificate is verified against roots (nil means the system
// pool) and the URL's host, no proxy is taken from the environment — an
// HTTPS_PROXY must not slip in between unannounced — and redirects are not
// followed (gorilla never does). headers is called on every dial, so a
// signature in it can be fresh.
func DialStrict(ctx context.Context, rawURL string, headers func(host, path string) http.Header,
	roots *x509.CertPool, handshakeTimeout time.Duration) (net.Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "wss" {
		return nil, fmt.Errorf("refusing %q: only wss:// carries this protocol", rawURL)
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: handshakeTimeout,
		ReadBufferSize:   BufferSize,
		WriteBufferSize:  BufferSize,
		Proxy:            nil,
		TLSClientConfig:  &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: u.Hostname()},
	}
	wsConn, _, err := dialer.DialContext(ctx, rawURL, headers(u.Host, u.RequestURI()))
	if err != nil {
		return nil, err
	}
	return Conn(wsConn), nil
}

// Listen accepts tunnel connections on addr, upgrading requests to path.
// An empty path means "/".
func Listen(addr, path string, handshakeTimeout time.Duration) (net.Listener, error) {
	tcpLn, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if path == "" {
		path = "/"
	}

	l := &listener{
		addr:  tcpLn.Addr(),
		conns: make(chan net.Conn),
		done:  make(chan struct{}),
	}
	up := Upgrader(handshakeTimeout)

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return // Upgrade has already answered
		}
		// Upgrade hijacks the connection, so returning from this handler does
		// not close it. Ownership passes to whoever takes it out of Accept.
		select {
		case l.conns <- Conn(wsConn):
		case <-l.done:
			wsConn.Close()
		}
	})

	l.srv = &http.Server{
		Handler: mux,
		// A peer that opens a connection and dawdles over the request line
		// cannot pin a goroutine indefinitely. No read or write timeout: once
		// upgraded, this connection is a long-lived tunnel.
		ReadHeaderTimeout: 10 * time.Second,
	}
	go l.srv.Serve(tcpLn)
	return l, nil
}

type listener struct {
	addr  net.Addr
	conns chan net.Conn
	srv   *http.Server
	done  chan struct{}
	once  sync.Once
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *listener) Addr() net.Addr { return l.addr }

func (l *listener) Close() error {
	var err error
	l.once.Do(func() {
		close(l.done)
		err = l.srv.Close()
	})
	return err
}

// The heartbeat is two text messages, outside the byte stream: tunnel data is
// only ever binary, so text cannot be mistaken for it. A peer that is a
// Cloudflare Durable Object answers PingText from the runtime without waking —
// a ping inside the tunnel would wake it every time — and records when it last
// did, which is how it tells a live client from a vanished one.
const (
	PingText = "vtunnel:ping"
	PongText = "vtunnel:pong"
)

// Conn presents a *websocket.Conn as a net.Conn.
//
// Reads stream directly from the WebSocket message reader to avoid
// allocations. Writes send each call as a single binary message. Text messages
// are the heartbeat and never reach the reader: a ping is answered, a pong
// noted, anything else dropped.
func Conn(ws *websocket.Conn) net.Conn {
	return &wsConn{Conn: ws, done: make(chan struct{})}
}

type wsConn struct {
	*websocket.Conn
	reader io.Reader

	// gorilla allows one concurrent reader and one concurrent writer, and
	// counts the deadline setters as read and write methods. A session
	// serializes its writes already, but that is its business, not a guarantee
	// to build on.
	writeMu sync.Mutex

	lastPong  atomic.Int64 // unix nanoseconds
	done      chan struct{}
	closeOnce sync.Once
}

func (c *wsConn) Read(dst []byte) (int, error) {
	for {
		if c.reader != nil {
			n, err := c.reader.Read(dst)
			if err == io.EOF {
				c.reader = nil
			}
			if n > 0 {
				return n, nil
			}
			if err != nil && err != io.EOF {
				return 0, err
			}
			continue
		}
		kind, r, err := c.Conn.NextReader()
		if err != nil {
			return 0, err
		}
		if kind == websocket.TextMessage {
			c.heartbeatMessage(r)
			continue
		}
		c.reader = r
	}
}

func (c *wsConn) heartbeatMessage(r io.Reader) {
	msg, err := io.ReadAll(io.LimitReader(r, 64))
	if err != nil {
		return
	}
	switch string(msg) {
	case PingText:
		c.writeMu.Lock()
		c.Conn.WriteMessage(websocket.TextMessage, []byte(PongText))
		c.writeMu.Unlock()
	case PongText:
		c.lastPong.Store(time.Now().UnixNano())
	}
}

// Heartbeat pings conn, a Conn, every interval, and closes it if no pong has
// arrived for timeout. It is for the dialling side of a link nothing else
// watches; it stops when conn closes.
func Heartbeat(conn net.Conn, interval, timeout time.Duration) {
	c, ok := conn.(*wsConn)
	if !ok || interval <= 0 {
		return
	}
	c.lastPong.Store(time.Now().UnixNano())
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-c.done:
				return
			case <-t.C:
			}
			if time.Since(time.Unix(0, c.lastPong.Load())) > timeout {
				c.Close()
				return
			}
			c.writeMu.Lock()
			err := c.Conn.WriteMessage(websocket.TextMessage, []byte(PingText))
			c.writeMu.Unlock()
			if err != nil {
				c.Close()
				return
			}
		}
	}()
}

func (c *wsConn) Close() error {
	c.closeOnce.Do(func() { close(c.done) })
	return c.Conn.Close()
}

func (c *wsConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.Conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *wsConn) SetWriteDeadline(t time.Time) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.Conn.SetWriteDeadline(t)
}

func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.Conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}
