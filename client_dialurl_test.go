package vtunnel

import (
	"net/url"
	"testing"
	"time"
)

// Every WebSocket upgrade carries the client's id, the same on every dial,
// and its heartbeat interval; whatever query the tunnel URL had stays.
func TestDialURLNamesTheClient(t *testing.T) {
	c := NewClient("wss://sandbox.example/ws/abc/tunnel?x=1", WithKeepAlive(5*time.Second))
	t.Cleanup(func() { c.Close() })

	first, err := url.Parse(c.dialURL())
	if err != nil {
		t.Fatal(err)
	}
	q := first.Query()
	if q.Get(clientParam) == "" {
		t.Fatalf("no %s in %s", clientParam, first)
	}
	if got := q.Get(keepAliveParam); got != "5000" {
		t.Errorf("%s = %q, want 5000", keepAliveParam, got)
	}
	if q.Get("x") != "1" || first.Path != "/ws/abc/tunnel" {
		t.Errorf("the tunnel URL's own path and query were not kept: %s", first)
	}
	if again := c.dialURL(); again != first.String() {
		t.Errorf("the second dial goes to %s, the first went to %s", again, first)
	}

	other := NewClient("wss://sandbox.example/ws/abc/tunnel")
	t.Cleanup(func() { other.Close() })
	o, _ := url.Parse(other.dialURL())
	if o.Query().Get(clientParam) == q.Get(clientParam) {
		t.Error("two clients share an id")
	}
}

// A client with its heartbeat turned off does not claim an interval.
func TestDialURLWithoutKeepAlive(t *testing.T) {
	c := NewClient("wss://sandbox.example/tunnel", WithKeepAlive(-1))
	t.Cleanup(func() { c.Close() })
	u, _ := url.Parse(c.dialURL())
	if u.Query().Has(keepAliveParam) {
		t.Errorf("%s sent with keepalive off: %s", keepAliveParam, u)
	}
}

// The query rides on WebSocket URLs only: ws:// as well as wss://, and not
// tcp://, which has none.
func TestDialURLSchemes(t *testing.T) {
	for raw, want := range map[string]bool{"ws://sandbox:3001/": true, "wss://sandbox/t": true, "tcp://sandbox:3001": false} {
		c := NewClient(raw)
		u, _ := url.Parse(c.dialURL())
		if got := u.Query().Has(clientParam); got != want {
			t.Errorf("%s: client id sent = %v, want %v", raw, got, want)
		}
		c.Close()
	}
}
