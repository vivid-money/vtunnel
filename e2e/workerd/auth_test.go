package workerd

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// sign makes an upgrade signature from the format itself, so the stand checks
// the format and not one implementation against itself.
func sign(key, host, path string, at time.Time) string {
	k, _ := base64.StdEncoding.DecodeString(key)
	nonce := make([]byte, 16)
	rand.Read(nonce)
	n := base64.RawURLEncoding.EncodeToString(nonce)
	ts := fmt.Sprint(at.Unix())
	m := hmac.New(sha256.New, k)
	fmt.Fprintf(m, "vtunnel-upgrade-v1\n%s\n%s\n%s\n%s", ts, n, strings.ToLower(host), path)
	return fmt.Sprintf("VTunnel-HMAC v1 ts=%s,nonce=%s,sig=%s", ts, n, base64.RawURLEncoding.EncodeToString(m.Sum(nil)))
}

// upgrade tries a WebSocket upgrade to path with auth, and says what status it
// got and why, closing the connection if it was accepted.
func (s *stand) upgrade(path, auth string) (int, string) {
	d := websocket.Dialer{TLSClientConfig: &tls.Config{RootCAs: https.Transport.(*http.Transport).TLSClientConfig.RootCAs}}
	h := http.Header{}
	if auth != "" {
		h.Set("Authorization", auth)
	}
	conn, res, err := d.Dial("wss://"+s.addr()+path, h)
	if conn != nil {
		conn.Close()
	}
	if res == nil {
		return 0, fmt.Sprint(err)
	}
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, strings.TrimSpace(string(body))
}

// Only a signed upgrade, for this host and path, now, once, opens a session.
// The controller count is the check that nothing got through.
func TestUpgradeAuth(t *testing.T) {
	s := newStand(t)
	host := s.addr()
	now := time.Now()
	wrongKey := base64.StdEncoding.EncodeToString(make([]byte, 32))

	replayed := sign(upgradeKey, host, "/tunnel", now)
	if code, body := s.upgrade("/tunnel", replayed); code != 101 {
		t.Fatalf("a good signature: %d %s", code, body)
	}

	for name, tc := range map[string]struct {
		path, auth, reason string
	}{
		"unsigned":             {"/tunnel", "", "missing"},
		"another key":          {"/tunnel", sign(wrongKey, host, "/tunnel", now), "bad_sig"},
		"signed for elsewhere": {"/tunnel?pod=x", sign(upgradeKey, host, "/tunnel", now), "bad_sig"},
		"another host":         {"/tunnel", sign(upgradeKey, "evil.example", "/tunnel", now), "bad_sig"},
		"stale":                {"/tunnel", sign(upgradeKey, host, "/tunnel", now.Add(-2*time.Minute)), "expired"},
		"replayed":             {"/tunnel", replayed, "replay"},
		"garbage":              {"/tunnel", "VTunnel-HMAC v1 nonsense", "malformed"},
	} {
		code, body := s.upgrade(tc.path, tc.auth)
		if code != 401 || !strings.HasSuffix(body, tc.reason) {
			t.Errorf("%s: %d %q, want 401 …%s", name, code, body, tc.reason)
		}
	}

	// A plain request is no way in either, signed or not.
	res, err := https.Get("https://" + host + "/tunnel")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Errorf("plain GET /tunnel: %d", res.StatusCode)
	}

	// The one good upgrade above was closed straight away; nothing else ever
	// became a session.
	s.waitFor("no controllers", func() bool { return s.status().Controllers == 0 })
}
