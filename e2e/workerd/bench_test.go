package workerd

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestBench is the benchmark through the whole Cloudflare-shaped path: a
// dynamic Worker's fetch() or connect() → the gateway → the tunnel object →
// yamux over WebSocket → a Go controller's MITM proxy → an upstream. It only
// runs when asked:
//
//	VTUNNEL_BENCH=1 go test ./workerd/ -run TestBench -v -count=1 [-bench.size=1GB]
//
// Time is taken here, around the request that makes the application do the
// work, after a warm-up that loads it.
func TestBench(t *testing.T) {
	if os.Getenv("VTUNNEL_BENCH") == "" {
		t.Skip("set VTUNNEL_BENCH=1 to run")
	}
	size := int64(1 << 30)
	if v := os.Getenv("VTUNNEL_BENCH_SIZE"); v != "" {
		fmt.Sscan(v, &size)
	}
	n := 2000

	s := newStand(t)
	s.addController()
	s.run("get") // load the application before timing anything

	fmt.Printf("\nvtunnel bench, workerd (dynamic Worker → gateway → Durable Object → Go controller), yamux-insecure over ws\n")
	fmt.Printf("  size: %s\n\n", fmtSize(size))

	throughput := func(name, c string) {
		start := time.Now()
		out := s.run(c, fmt.Sprintf("n=%d", size))
		elapsed := time.Since(start)
		total := int64(out["total"].(float64))
		if total < size {
			t.Errorf("%s moved %d of %d bytes", name, total, size)
		}
		fmt.Printf("--- %s ---\n  %s in %v\n  throughput: %s/s\n\n", name, fmtSize(total), elapsed.Round(time.Millisecond),
			fmtSize(int64(float64(total)/elapsed.Seconds())))
	}
	ops := func(name, c string, query ...string) {
		start := time.Now()
		s.run(c, query...)
		elapsed := time.Since(start)
		fmt.Printf("--- %s ---\n  %d ops in %v\n  %v per op (%.0f ops/s)\n\n", name, n, elapsed.Round(time.Millisecond),
			(elapsed / time.Duration(n)).Round(time.Microsecond), float64(n)/elapsed.Seconds())
	}

	fmt.Printf("=== fetch ===\n\n")
	throughput("download (response body)", "download")
	throughput("upload (POST body)", "upload")
	fmt.Printf("=== connect ===\n\n")
	throughput("download (raw TCP)", "tcpdownload")
	fmt.Printf("=== requests (fetch, 204) ===\n\n")
	ops(fmt.Sprintf("%d requests, 1 at a time", n), "requests", fmt.Sprintf("n=%d", n), "c=1")
	ops(fmt.Sprintf("%d requests, 32 at a time", n), "requests", fmt.Sprintf("n=%d", n), "c=32")
}

func fmtSize(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
