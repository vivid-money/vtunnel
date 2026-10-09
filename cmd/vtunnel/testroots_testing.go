//go:build vtunnel_testing

package main

import (
	"crypto/x509"
	"flag"
	"log"
	"os"

	"github.com/vivid-money/vtunnel"
)

// testRootsFlag defines -test-ca, in test builds only: the PEM certificates a
// yamux-wss client trusts instead of the system roots, for a local server with
// a certificate of its own (wrangler dev --local-protocol https).
func testRootsFlag(fs *flag.FlagSet) func() []vtunnel.Option {
	path := fs.String("test-ca", "", "PEM roots a yamux-wss client trusts instead of the system's (test builds only)")
	return func() []vtunnel.Option {
		if *path == "" {
			return nil
		}
		pem, err := os.ReadFile(*path)
		if err != nil {
			log.Fatalf("[vtunnel] -test-ca: %v", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			log.Fatalf("[vtunnel] -test-ca: no certificates in %s", *path)
		}
		log.Printf("[vtunnel] TEST BUILD: trusting %s for wss:// instead of the system roots", *path)
		return []vtunnel.Option{vtunnel.WithTestRootCAs(pool)}
	}
}
