//go:build vtunnel_testing

package main

import (
	"crypto/x509"
	"log"
	"os"

	"github.com/vivid-money/vtunnel"
)

// testRoots trusts the PEM certificates in path for wss://.
func testRoots(path string) vtunnel.Option {
	pem, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		log.Fatalf("no certificates in %s", path)
	}
	return vtunnel.WithTestRootCAs(pool)
}
