package vtunnel

import (
	"bufio"
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// goldenKey and the signature below were computed with Python's hmac, not with
// this code, so that this test and its TypeScript twin (ts/test/upgrade.test.ts)
// are checked against something neither wrote.
const goldenKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="

func TestUpgradeMACMatchesGoldenVector(t *testing.T) {
	key, err := ParseUpgradeKey(goldenKey)
	if err != nil {
		t.Fatal(err)
	}
	got := upgradeMAC(key, "1760000000", "AAAAAAAAAAAAAAAAAAAAAA", "Example.Workers.Dev", "/tunnel?pod=a")
	if want := "6iNrjxS_e-78YQFJiYsLipkW842_ebLjaLgPIUR38EI"; got != want {
		t.Fatalf("sig = %s, want %s", got, want)
	}
}

func TestParseUpgradeKey(t *testing.T) {
	for _, ok := range []string{goldenKey, strings.TrimRight(goldenKey, "="), "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"} {
		if _, err := ParseUpgradeKey(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "short", "AAECAwQFBgcICQoLDA0ODw==", "not base64 at all!!"} {
		if _, err := ParseUpgradeKey(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestUpgradeVerifier(t *testing.T) {
	key, _ := ParseUpgradeKey(goldenKey)
	now := time.Unix(1760000000, 0)
	v, _ := NewUpgradeVerifier(goldenKey)
	v.now = func() time.Time { return now }

	sign := func(host, path string, at time.Time) string { return signUpgrade(key, host, path, at) }
	reason := func(err error) string {
		var ue *UpgradeError
		if errors.As(err, &ue) {
			return ue.Reason
		}
		return "ok"
	}

	good := sign("tunnel.example", "/tunnel", now)
	if r := reason(v.verify(good, "TUNNEL.example", "/tunnel")); r != "ok" {
		t.Fatalf("a good signature: %s", r)
	}
	if r := reason(v.verify(good, "tunnel.example", "/tunnel")); r != "replay" {
		t.Errorf("the same signature again: %s, want replay", r)
	}

	for name, tc := range map[string]struct {
		auth, host, path, want string
	}{
		"another host":      {sign("tunnel.example", "/tunnel", now), "evil.example", "/tunnel", "bad_sig"},
		"another path":      {sign("tunnel.example", "/tunnel", now), "tunnel.example", "/tunnel?x", "bad_sig"},
		"too old":           {sign("tunnel.example", "/tunnel", now.Add(-61*time.Second)), "tunnel.example", "/tunnel", "expired"},
		"from the future":   {sign("tunnel.example", "/tunnel", now.Add(61*time.Second)), "tunnel.example", "/tunnel", "expired"},
		"just inside":       {sign("tunnel.example", "/tunnel", now.Add(-59*time.Second)), "tunnel.example", "/tunnel", "ok"},
		"missing":           {"", "tunnel.example", "/tunnel", "missing"},
		"another scheme":    {"Bearer x", "tunnel.example", "/tunnel", "malformed"},
		"short nonce":       {"VTunnel-HMAC v1 ts=1760000000,nonce=abc,sig=x", "tunnel.example", "/tunnel", "malformed"},
		"another key's sig": {signUpgrade(make([]byte, 32), "tunnel.example", "/tunnel", now), "tunnel.example", "/tunnel", "bad_sig"},
	} {
		if r := reason(v.verify(tc.auth, tc.host, tc.path)); r != tc.want {
			t.Errorf("%s: %s, want %s", name, r, tc.want)
		}
	}
}

// The client refuses every way of running ProtocolYamuxWSS that would leave it
// without its security, before dialling anything.
func TestYamuxWSSClientRefusals(t *testing.T) {
	dialled := false
	custom := func(d Dialer) Option { return WithDialer(d) }
	for name, c := range map[string]*Client{
		"no key":        NewClient("wss://tunnel.example/", WithProtocol(ProtocolYamuxWSS)),
		"bad key":       NewClient("wss://tunnel.example/", WithProtocol(ProtocolYamuxWSS), WithUpgradeKey("short")),
		"ws://":         NewClient("ws://tunnel.example/", WithProtocol(ProtocolYamuxWSS), WithUpgradeKey(goldenKey)),
		"tcp://":        NewClient("tcp://tunnel.example:1", WithProtocol(ProtocolYamuxWSS), WithUpgradeKey(goldenKey)),
		"custom dialer": NewClient("wss://tunnel.example/", WithProtocol(ProtocolYamuxWSS), WithUpgradeKey(goldenKey), custom(func(ctx context.Context) (net.Conn, error) { dialled = true; return nil, io.EOF })),
	} {
		if err := c.Connect(); err == nil {
			t.Errorf("%s: connected", name)
		} else {
			t.Logf("%s: %v", name, err)
		}
		c.Close()
	}
	if dialled {
		t.Error("the custom dialer was used")
	}
}

// End to end against a Go server: TLS in front, the upgrade verified before it
// is accepted, yamux-wss behind. A wrong key and an untrusted certificate both
// keep the tunnel down.
func TestYamuxWSSEndToEnd(t *testing.T) {
	verifier, _ := NewUpgradeVerifier(goldenKey)
	server := NewServer(WithServerProtocol(ProtocolYamuxWSS))
	defer server.Close()
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	refused := make(chan string, 10)
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := verifier.Verify(r); err != nil {
			refused <- err.(*UpgradeError).Reason
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		server.HandleWebSocket(conn)
	}))
	defer ts.Close()
	url := "wss" + strings.TrimPrefix(ts.URL, "https") + "/tunnel"
	roots := x509.NewCertPool()
	roots.AddCert(ts.Certificate())

	client := NewClient(url, WithProtocol(ProtocolYamuxWSS), WithUpgradeKey(goldenKey), WithKeepAlive(-1))
	client.testRoots = roots
	if err := client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	echo, _ := net.Listen("tcp", "127.0.0.1:0")
	defer echo.Close()
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	port := freeTestPort(t)
	if err := client.Listen(port, echo.Addr().String()); err != nil {
		t.Fatal(err)
	}
	var conn net.Conn
	deadline := time.Now().Add(3 * time.Second)
	for {
		var err error
		if conn, err = net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port))); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer conn.Close()
	io.WriteString(conn, "through yamux-wss\n")
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || line != "through yamux-wss\n" {
		t.Fatalf("echo: %q, %v", line, err)
	}

	wrong := NewClient(url, WithProtocol(ProtocolYamuxWSS), WithUpgradeKey("AQECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="))
	wrong.testRoots = roots
	if err := wrong.Connect(); err == nil {
		t.Error("connected with another key")
	} else if r := <-refused; r != "bad_sig" {
		t.Errorf("refused as %s, want bad_sig", r)
	}
	wrong.Close()

	untrusting := NewClient(url, WithProtocol(ProtocolYamuxWSS), WithUpgradeKey(goldenKey))
	if err := untrusting.Connect(); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Errorf("an unverifiable certificate: %v", err)
	}
	untrusting.Close()
}

func freeTestPort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
