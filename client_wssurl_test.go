package vtunnel

import (
	"net/url"
	"testing"
	"time"
)

// Every yamux-wss upgrade carries the client's id, the same on every dial,
// and its heartbeat interval; whatever query the tunnel URL had stays.
func TestWSSURLNamesTheClient(t *testing.T) {
	c := NewClient("wss://sandbox.example/ws/abc/tunnel?x=1", WithKeepAlive(5*time.Second))
	t.Cleanup(func() { c.Close() })

	first, err := url.Parse(c.wssURL())
	if err != nil {
		t.Fatal(err)
	}
	q := first.Query()
	if q.Get(wssClientParam) == "" {
		t.Fatalf("no %s in %s", wssClientParam, first)
	}
	if got := q.Get(wssKeepAliveParam); got != "5000" {
		t.Errorf("%s = %q, want 5000", wssKeepAliveParam, got)
	}
	if q.Get("x") != "1" || first.Path != "/ws/abc/tunnel" {
		t.Errorf("the tunnel URL's own path and query were not kept: %s", first)
	}
	if again := c.wssURL(); again != first.String() {
		t.Errorf("the second dial goes to %s, the first went to %s", again, first)
	}

	other := NewClient("wss://sandbox.example/ws/abc/tunnel")
	t.Cleanup(func() { other.Close() })
	o, _ := url.Parse(other.wssURL())
	if o.Query().Get(wssClientParam) == q.Get(wssClientParam) {
		t.Error("two clients share an id")
	}
}

// A client with its heartbeat turned off does not claim an interval.
func TestWSSURLWithoutKeepAlive(t *testing.T) {
	c := NewClient("wss://sandbox.example/tunnel", WithKeepAlive(-1))
	t.Cleanup(func() { c.Close() })
	u, _ := url.Parse(c.wssURL())
	if u.Query().Has(wssKeepAliveParam) {
		t.Errorf("%s sent with keepalive off: %s", wssKeepAliveParam, u)
	}
}
