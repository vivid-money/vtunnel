//go:build vtunnel_testing

package vtunnel

import "crypto/x509"

// WithTestRootCAs makes [ProtocolYamuxWSS] trust roots instead of the system
// pool. It exists only in builds tagged vtunnel_testing, for stands with a
// certificate of their own; no configuration can reach it, which is the point —
// the protocol's server authentication is that certificate check and nothing
// else.
func WithTestRootCAs(roots *x509.CertPool) Option {
	return func(c *Client) {
		c.testRoots = roots
	}
}
