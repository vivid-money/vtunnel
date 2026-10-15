// Command vtclient is the controller in the TypeScript compatibility tests
// (ts/test/server-compat.test.ts, ts/test/egress-compat.test.ts): a real
// vtunnel.Client, yamux-insecure, dialling the TypeScript end over a
// WebSocket, with a MITM proxy serving these routes —
//
//   - fwd.test: forwarded to a cleartext upstream in this process, with a
//     header injected on the way, which is the job the tunnel exists to do;
//   - tls.test: the same upstream over TLS ("tls://"), so the request crosses
//     the tunnel in the clear and is encrypted by this proxy;
//   - api.test: answered by a handler here, with a path per response shape —
//     /sse, /chunked, /status/N, /big?n=N, /slow;
//   - tcp.test: piped to a raw TCP echo server, for CONNECT;
//   - sink.test, source.test, tcpsink.test: for throughput — a handler that
//     counts what it is sent, a raw TCP server that sends zeros until closed,
//     and one that reads and discards.
//
// With -handshakes N it instead connects and disconnects N times, prints the
// time per handshake and exits: the benchmark's handshake mode.
// Request-describing answers are JSON, so the test can check what crossed the
// tunnel. It prints "ready" once routes and policy are through, then runs until killed, reconnecting as a controller does. Anything
// fatal goes to stderr and exits non-zero.
package main

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vivid-money/vtunnel"
)

func main() {
	url := flag.String("url", "", "ws:// or wss:// URL of the TypeScript end")
	handshakes := flag.Int("handshakes", 0, "connect and close this many times, report, exit")
	protocol := flag.String("protocol", string(vtunnel.ProtocolYamuxInsecure), "session protocol")
	upgradeKey := flag.String("upgrade-key", "", "upgrade key, for yamux-wss")
	keepAlive := flag.Duration("keepalive", 0, "keepalive interval (0 = the default)")
	caFile := flag.String("ca", "", "PEM roots to trust for wss:// (test builds only: -tags vtunnel_testing)")
	flag.Parse()
	log.SetFlags(0)

	if *handshakes > 0 {
		log.SetOutput(io.Discard)
		start := time.Now()
		for range *handshakes {
			c := vtunnel.NewClient(*url, vtunnel.WithProtocol(vtunnel.ProtocolYamuxInsecure), vtunnel.WithKeepAlive(-1))
			if err := c.Connect(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			c.Close()
		}
		fmt.Printf("%d\n", time.Since(start).Nanoseconds()/int64(*handshakes))
		return
	}

	// Hanging is a failure, and must not outlive the test run.
	time.AfterFunc(90*time.Second, func() { log.Fatal("timed out") })

	describeUpstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { describe(w, "upstream", r) })
	upstream := httptest.NewServer(describeUpstream)
	defer upstream.Close()
	tlsUpstream := httptest.NewTLSServer(describeUpstream)
	defer tlsUpstream.Close()
	echo := tcpEcho()

	blob, err := vtunnel.GenerateCA("vtclient test CA")
	if err != nil {
		log.Fatal(err)
	}
	ca, err := vtunnel.LoadCA(blob)
	if err != nil {
		log.Fatal(err)
	}

	opts := []vtunnel.Option{
		vtunnel.WithProtocol(vtunnel.Protocol(*protocol)),
		vtunnel.WithMitm(ca),
		vtunnel.WithReconnectBackoff(20*time.Millisecond, 100*time.Millisecond),
	}
	if *upgradeKey != "" {
		opts = append(opts, vtunnel.WithUpgradeKey(*upgradeKey))
	}
	if *keepAlive > 0 {
		opts = append(opts, vtunnel.WithKeepAlive(*keepAlive))
	}
	if *caFile != "" {
		opts = append(opts, testRoots(*caFile))
	}
	client := vtunnel.NewClient(*url, opts...)
	routes := client.Proxy()
	pool := x509.NewCertPool()
	pool.AddCert(tlsUpstream.Certificate())
	routes.SetTransportTLSConfig(&tls.Config{RootCAs: pool})

	must(routes.ForwardTo("fwd.test", "http://"+upstream.Listener.Addr().String(),
		vtunnel.WithHeader("Authorization", "Bearer injected")))
	must(routes.ForwardTo("tls.test", "tls://"+tlsUpstream.Listener.Addr().String(),
		vtunnel.WithHeader("Authorization", "Bearer injected-tls")))
	must(routes.Handle("api.test", http.HandlerFunc(api)))
	must(routes.ForwardTo("tcp.test", echo))
	must(routes.Handle("sink.test", http.HandlerFunc(sink)))
	must(routes.ForwardTo("source.test", tcpSource()))
	must(routes.ForwardTo("tcpsink.test", tcpSink()))

	must(client.Connect())
	// The TypeScript end decides nothing about direct egress — the code in
	// front of it does — so it refuses every policy, and SetEgressPolicy has
	// to say so rather than report one applied.
	if err := client.SetEgressPolicy(vtunnel.Policy{Default: vtunnel.ActionDeny}); err == nil {
		log.Fatal("policy: accepted by an end that enforces none")
	}
	// Set up and answered: the test may start pulling the connection about.
	fmt.Println("ready")

	select {}
}

func api(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/sse":
		w.Header().Set("Content-Type", "text/event-stream")
		for i := range 3 {
			fmt.Fprintf(w, "data: %d\n\n", i)
			w.(http.Flusher).Flush()
			time.Sleep(200 * time.Millisecond)
		}
	case r.URL.Path == "/chunked":
		w.Header().Set("Trailer", "X-Checksum")
		for i := range 5 {
			fmt.Fprintf(w, "part %d;", i)
			w.(http.Flusher).Flush()
		}
		w.Header().Set("X-Checksum", "ok")
	case strings.HasPrefix(r.URL.Path, "/status/"):
		code, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/status/"))
		w.Header().Set("X-Status", strconv.Itoa(code))
		w.WriteHeader(code)
		if code != http.StatusNoContent && code != http.StatusNotModified {
			io.WriteString(w, "status body")
		}
	case r.URL.Path == "/big":
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		w.Header().Set("Content-Length", strconv.Itoa(n))
		buf := make([]byte, 64*1024)
		for i := range buf {
			buf[i] = byte(i)
		}
		for n > 0 {
			k := min(n, len(buf))
			w.Write(buf[:k])
			n -= k
		}
	case r.URL.Path == "/slow":
		time.Sleep(2 * time.Second)
		io.WriteString(w, "late")
	case r.URL.Path == "/cookies":
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
	default:
		describe(w, "handler", r)
	}
}

// sink counts the request body and says how much it got.
func sink(w http.ResponseWriter, r *http.Request) {
	n, err := io.Copy(io.Discard, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"n":%d}`, n)
}

func describe(w http.ResponseWriter, who string, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sum := sha256.Sum256(body)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"who":              who,
		"method":           r.Method,
		"host":             r.Host,
		"uri":              r.URL.RequestURI(),
		"tls":              r.TLS != nil,
		"authorization":    r.Header.Get("Authorization"),
		"x-test":           r.Header.Get("X-Test"),
		"contentLength":    r.ContentLength,
		"transferEncoding": r.TransferEncoding,
		"bodyLength":       len(body),
		"bodySha256":       hex.EncodeToString(sum[:]),
	})
}

// tcpEcho serves raw TCP: a greeting first, then everything echoed back, and
// a half-close answered by a final line before closing.
func tcpEcho() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				io.WriteString(c, "HELLO\n")
				io.Copy(c, c)
				io.WriteString(c, "BYE\n")
			}()
		}
	}()
	return ln.Addr().String()
}

// tcpSource serves raw TCP that writes zeros until the peer goes away.
func tcpSource() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		buf := make([]byte, 64*1024)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					if _, err := c.Write(buf); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// tcpSink serves raw TCP that reads and discards.
func tcpSink() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				io.Copy(io.Discard, c)
			}()
		}
	}()
	return ln.Addr().String()
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
