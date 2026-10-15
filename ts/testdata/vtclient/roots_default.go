//go:build !vtunnel_testing

package main

import (
	"log"

	"github.com/vivid-money/vtunnel"
)

// testRoots is refused outside test builds, as WithTestRootCAs does not exist
// in them: build with -tags vtunnel_testing.
func testRoots(string) vtunnel.Option {
	log.Fatal("-ca needs a test build: go build -tags vtunnel_testing")
	return nil
}
