//go:build !vtunnel_testing

package main

import (
	"flag"

	"github.com/vivid-money/vtunnel"
)

// testRootsFlag defines nothing outside test builds: trusting roots other than
// the system's is not something a release binary can be told to do.
func testRootsFlag(*flag.FlagSet) func() []vtunnel.Option {
	return func() []vtunnel.Option { return nil }
}
