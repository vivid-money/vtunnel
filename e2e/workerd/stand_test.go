// Package workerd runs the TypeScript sandbox side in workerd, the runtime
// Cloudflare runs, against real Go controllers.
//
//	go test ./workerd/... (from e2e/)
//
// It needs bun (to bundle stand/worker.ts) and npm (for the workerd binary),
// and skips without them. Everything else it builds itself.
package workerd

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	repoRoot   string
	workerdBin string
	bundle     string
	controller string // testdata/vtclient, built with -tags vtunnel_testing
	certFile   string // the stand's TLS certificate, which controllers and this test trust
	https      *http.Client

	// upgradeKey is what controllers sign their upgrade with, and the
	// stand's VTUNNEL_UPGRADE_KEY.
	upgradeKey = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
)

func TestMain(m *testing.M) {
	code, err := setUp(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(code)
}

func setUp(m *testing.M) (int, error) {
	if _, err := exec.LookPath("bun"); err != nil {
		fmt.Println("skipping the workerd stand: bun not found")
		return 0, nil
	}
	if _, err := exec.LookPath("npm"); err != nil {
		fmt.Println("skipping the workerd stand: npm not found")
		return 0, nil
	}
	here, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	repoRoot = filepath.Join(here, "..", "..")
	tmp, err := os.MkdirTemp("", "vtunnel-workerd-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tmp)

	steps := []struct {
		dir  string
		args []string
	}{
		{here, []string{"npm", "install", "--no-audit", "--no-fund", "--silent"}},
		{filepath.Join(repoRoot, "ts"), []string{"bun", "install", "--silent"}},
		{here, []string{"bun", "build", "stand/worker.ts", "--target=browser", "--format=esm",
			"--external", "cloudflare:*", "--outfile", filepath.Join(tmp, "worker.js")}},
		{repoRoot, []string{"go", "build", "-tags", "vtunnel_testing", "-o", filepath.Join(tmp, "vtclient"), "./ts/testdata/vtclient"}},
	}
	for _, s := range steps {
		cmd := exec.Command(s.args[0], s.args[1:]...)
		cmd.Dir = s.dir
		if out, err := cmd.CombinedOutput(); err != nil {
			return 0, fmt.Errorf("%s: %v\n%s", strings.Join(s.args, " "), err, out)
		}
	}
	workerdBin = filepath.Join(here, "node_modules", ".bin", "workerd")
	bundle = filepath.Join(tmp, "worker.js")
	controller = filepath.Join(tmp, "vtclient")
	if err := writeKeypair(tmp); err != nil {
		return 0, err
	}
	return m.Run(), nil
}

// writeKeypair makes the stand's certificate, for 127.0.0.1, next to the
// bundle where the config embeds it from, and an HTTP client that trusts it.
func writeKeypair(dir string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "vtunnel workerd stand"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	certFile = filepath.Join(dir, "cert.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return err
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	https = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	return nil
}

// stand is one workerd process serving the stand Worker, and the controllers
// dialled into it.
type stand struct {
	t        *testing.T
	port     int
	config   string
	workerd  *exec.Cmd
	wlog     *syncBuffer
	ctrls    []*exec.Cmd
	ctrlLogs []*syncBuffer
}

func newStand(t *testing.T) *stand {
	t.Helper()
	s := &stand{t: t, port: freePort(t)}
	tmpl, err := os.ReadFile("stand/config.capnp.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	// Next to the bundle, which capnp's embed only finds by a relative path.
	cfg := strings.NewReplacer("{{PORT}}", fmt.Sprint(s.port), "{{BUNDLE}}", filepath.Base(bundle),
		"{{UPGRADE_KEY}}", upgradeKey).Replace(string(tmpl))
	s.config = filepath.Join(filepath.Dir(bundle), fmt.Sprintf("config-%d.capnp", s.port))
	if err := os.WriteFile(s.config, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	s.startWorkerd()
	t.Cleanup(s.stop)
	return s
}

func (s *stand) startWorkerd() {
	s.t.Helper()
	s.wlog = &syncBuffer{}
	s.workerd = exec.Command(workerdBin, "serve", s.config, "--experimental")
	s.workerd.Stdout, s.workerd.Stderr = s.wlog, s.wlog
	if err := s.workerd.Start(); err != nil {
		s.t.Fatal(err)
	}
	s.waitFor("workerd listening", func() bool {
		c, err := net.Dial("tcp", s.addr())
		if err == nil {
			c.Close()
		}
		return err == nil
	})
}

// addController starts one more controller and waits until it is routed.
func (s *stand) addController() {
	s.t.Helper()
	logs := &syncBuffer{}
	cmd := exec.Command(controller, "-url", s.tunnelURL(), "-protocol", "yamux-wss",
		"-upgrade-key", upgradeKey, "-ca", certFile)
	cmd.Stderr = logs
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		s.t.Fatal(err)
	}
	s.ctrls = append(s.ctrls, cmd)
	s.ctrlLogs = append(s.ctrlLogs, logs)
	ready := make(chan bool, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if sc.Text() == "ready" {
				ready <- true
				io.Copy(io.Discard, stdout)
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			s.t.Fatalf("controller died:\n%s", logs)
		}
	case <-time.After(15 * time.Second):
		s.t.Fatalf("controller not ready:\n%s", logs)
	}
	want := len(s.ctrls)
	s.waitFor(fmt.Sprintf("%d controllers", want), func() bool { return s.status().Controllers == want })
}

func (s *stand) addr() string { return fmt.Sprintf("127.0.0.1:%d", s.port) }

func (s *stand) tunnelURL() string { return fmt.Sprintf("wss://%s/tunnel", s.addr()) }

type status struct {
	Controllers int  `json:"controllers"`
	Routes      bool `json:"routes"`
}

func (s *stand) status() status {
	var st status
	res, err := https.Get("https://" + s.addr() + "/status")
	if err != nil {
		return st
	}
	defer res.Body.Close()
	json.NewDecoder(res.Body).Decode(&st)
	return st
}

// run has the application run one case and decodes what it answered.
func (s *stand) run(c string, query ...string) map[string]any {
	s.t.Helper()
	out := s.tryRun(c, query...)
	if _, threw := out["threw"]; threw {
		s.t.Fatalf("run %s: %v\nworkerd:\n%s", c, out, s.wlog)
	}
	return out
}

// tryRun is run, except that a case that threw is an answer, not a failure.
func (s *stand) tryRun(c string, query ...string) map[string]any {
	s.t.Helper()
	q := strings.Join(append([]string{"case=" + c}, query...), "&")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://"+s.addr()+"/run?"+q, nil)
	res, err := https.Do(req)
	if err != nil {
		s.t.Fatalf("run %s: %v\nworkerd:\n%s", c, err, s.wlog)
	}
	defer res.Body.Close()
	var out map[string]any
	body, _ := io.ReadAll(res.Body)
	if err := json.Unmarshal(body, &out); err != nil {
		s.t.Fatalf("run %s: %v: %s", c, err, body)
	}
	return out
}

func (s *stand) waitFor(what string, cond func() bool) {
	s.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out waiting for %s\nworkerd:\n%s", what, s.wlog)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (s *stand) stop() {
	for _, c := range s.ctrls {
		c.Process.Kill()
		c.Wait()
	}
	if s.workerd != nil {
		s.workerd.Process.Kill()
		s.workerd.Wait()
	}
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// get digs a value out of decoded JSON by path: get(m, "body", "uri").
func get(v any, path ...string) any {
	for _, p := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[p]
	}
	return v
}
