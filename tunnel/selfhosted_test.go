package tunnel

import "testing"

func TestBuildLocalURL(t *testing.T) {
	cases := []struct {
		name       string
		listenAddr string
		tls        bool
		want       string
	}{
		{"wildcard shorthand", ":8080", false, "http://127.0.0.1:8080"},
		{"explicit wildcard", "0.0.0.0:8080", false, "http://127.0.0.1:8080"},
		{"ipv6 wildcard", "[::]:8080", false, "http://127.0.0.1:8080"},
		{"wildcard with tls", ":8443", true, "https://127.0.0.1:8443"},
		{
			// A socket bound to a specific non-wildcard address doesn't
			// accept connections on 127.0.0.1 — the internal client must
			// dial the address actually bound to, not assume loopback.
			"specific LAN address", "192.168.0.4:18081", false, "http://192.168.0.4:18081",
		},
		{"specific address with tls", "192.168.0.4:8443", true, "https://192.168.0.4:8443"},
		{"unparseable falls back verbatim", "not-a-valid-addr", false, "http://not-a-valid-addr"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := buildLocalURL(c.listenAddr, c.tls); got != c.want {
				t.Errorf("buildLocalURL(%q, %v) = %q, want %q", c.listenAddr, c.tls, got, c.want)
			}
		})
	}
}
