package main

import (
	"testing"

	"github.com/vivid-money/vtunnel"
)

func TestProtocolFor(t *testing.T) {
	for _, tc := range []struct {
		v      string
		client bool
		want   vtunnel.Protocol
		err    bool
	}{
		{"", true, vtunnel.ProtocolSSH, false},
		{"", false, vtunnel.ProtocolSSH, false},
		{"yamux", false, vtunnel.ProtocolYamux, false},
		{"yamux-insecure", true, vtunnel.ProtocolYamuxInsecure, false},
		{"yamux-wss", true, vtunnel.ProtocolYamuxWSS, false},
		// The server command has no way to check a signed upgrade.
		{"yamux-wss", false, "", true},
		{"ssh2", true, "", true},
	} {
		got, err := protocolFor(tc.v, tc.client)
		if got != tc.want || (err != nil) != tc.err {
			t.Errorf("protocolFor(%q, client=%v) = %q, %v", tc.v, tc.client, got, err)
		}
	}
}
