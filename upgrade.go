package vtunnel

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The signed upgrade is how [ProtocolYamuxWSS] authenticates the client. With
// no TLS of its own inside the tunnel, the session cannot prove who dialled, so
// the WebSocket upgrade request carries the proof instead:
//
//	Authorization: VTunnel-HMAC v1 ts=<unix seconds>,nonce=<16 bytes, base64url>,sig=<base64url>
//	sig = HMAC-SHA256(upgrade key, "vtunnel-upgrade-v1\n" ts "\n" nonce "\n" host "\n" path)
//
// It is bound to the host and path it was made for, valid for upgradeWindow
// either side of the verifier's clock, and good once: a verifier remembers the
// nonces it has accepted for as long as they could be replayed.
//
// The key is not the tunnel secret. A tunnel secret is stretched with Argon2id,
// which the far end of this protocol — a Cloudflare Durable Object — has no way
// to compute; and a value kept in two places should be one whose leak costs
// only the one door it opens. So it is its own random value, configured on
// both ends: [WithUpgradeKey] here, VTUNNEL_UPGRADE_KEY there. Thirty-two
// random bytes, base64:
//
//	openssl rand -base64 32

const (
	upgradeScheme  = "VTunnel-HMAC"
	upgradeVersion = "v1"
	upgradeLabel   = "vtunnel-upgrade-v1"

	// upgradeWindow is how far a signature's time may be from the verifier's.
	upgradeWindow = 60 * time.Second

	// minUpgradeKey is the shortest key accepted, in bytes: a full HMAC-SHA256
	// block's worth of what it is keyed with.
	minUpgradeKey = 32
)

// ParseUpgradeKey reads an upgrade key written as base64, standard or URL
// alphabet, padded or not.
func ParseUpgradeKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if key, err := enc.DecodeString(s); err == nil {
			if len(key) < minUpgradeKey {
				return nil, fmt.Errorf("vtunnel: upgrade key is %d bytes, want at least %d (openssl rand -base64 32)", len(key), minUpgradeKey)
			}
			return key, nil
		}
	}
	return nil, errors.New("vtunnel: upgrade key is not base64 (openssl rand -base64 32)")
}

// signUpgrade returns the Authorization value for an upgrade to host and path,
// made at now.
func signUpgrade(key []byte, host, path string, now time.Time) string {
	var raw [16]byte
	rand.Read(raw[:])
	nonce := base64.RawURLEncoding.EncodeToString(raw[:])
	ts := strconv.FormatInt(now.Unix(), 10)
	sig := upgradeMAC(key, ts, nonce, host, path)
	return fmt.Sprintf("%s %s ts=%s,nonce=%s,sig=%s", upgradeScheme, upgradeVersion, ts, nonce, sig)
}

func upgradeMAC(key []byte, ts, nonce, host, path string) string {
	m := hmac.New(sha256.New, key)
	fmt.Fprintf(m, "%s\n%s\n%s\n%s\n%s", upgradeLabel, ts, nonce, strings.ToLower(host), path)
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// UpgradeVerifier checks signed upgrades: the server half of
// [ProtocolYamuxWSS], for a Go endpoint. Call Verify before upgrading, and
// refuse the request if it fails.
type UpgradeVerifier struct {
	key []byte
	now func() time.Time

	mu   sync.Mutex
	seen map[string]time.Time // nonce -> when it can no longer be replayed
}

// NewUpgradeVerifier parses key as [ParseUpgradeKey] does.
func NewUpgradeVerifier(key string) (*UpgradeVerifier, error) {
	k, err := ParseUpgradeKey(key)
	if err != nil {
		return nil, err
	}
	return &UpgradeVerifier{key: k, now: time.Now, seen: make(map[string]time.Time)}, nil
}

// UpgradeError says why a signed upgrade was refused. Reason is one of
// "missing", "malformed", "expired", "bad_sig" and "replay", for counting.
type UpgradeError struct{ Reason string }

func (e *UpgradeError) Error() string { return "vtunnel: upgrade refused: " + e.Reason }

// Verify checks r's signature against the host and path r was sent to.
func (v *UpgradeVerifier) Verify(r *http.Request) error {
	return v.verify(r.Header.Get("Authorization"), r.Host, r.URL.RequestURI())
}

func (v *UpgradeVerifier) verify(auth, host, path string) error {
	if auth == "" {
		return &UpgradeError{"missing"}
	}
	ts, nonce, sig, ok := parseUpgradeAuth(auth)
	if !ok {
		return &UpgradeError{"malformed"}
	}
	secs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return &UpgradeError{"malformed"}
	}
	now := v.now()
	when := time.Unix(secs, 0)
	if when.Before(now.Add(-upgradeWindow)) || when.After(now.Add(upgradeWindow)) {
		return &UpgradeError{"expired"}
	}
	want := upgradeMAC(v.key, ts, nonce, host, path)
	if !hmac.Equal([]byte(sig), []byte(want)) {
		return &UpgradeError{"bad_sig"}
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	for n, until := range v.seen {
		if now.After(until) {
			delete(v.seen, n)
		}
	}
	if _, used := v.seen[nonce]; used {
		return &UpgradeError{"replay"}
	}
	// Past this, the timestamp alone refuses it.
	v.seen[nonce] = when.Add(upgradeWindow)
	return nil
}

func parseUpgradeAuth(auth string) (ts, nonce, sig string, ok bool) {
	rest, found := strings.CutPrefix(auth, upgradeScheme+" "+upgradeVersion+" ")
	if !found {
		return "", "", "", false
	}
	for part := range strings.SplitSeq(rest, ",") {
		k, val, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			return "", "", "", false
		}
		switch k {
		case "ts":
			ts = val
		case "nonce":
			nonce = val
		case "sig":
			sig = val
		}
	}
	if ts == "" || len(nonce) < 16 || sig == "" {
		return "", "", "", false
	}
	return ts, nonce, sig, true
}
