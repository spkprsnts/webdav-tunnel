package tunnel

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

// isGREASE reports whether v is a GREASE value (RFC 8701): 0x?a?a.
func isGREASE(v uint16) bool { return v&0x0f0f == 0x0a0a && v>>8 == v&0xff }

// The Chrome dialer must send a Chrome-shaped ClientHello (GREASE, h2 removed
// from ALPN) and still carry HTTP/1.1 requests through http.Transport, even
// against a server that would prefer HTTP/2.
func TestChromeFingerprintHTTP1(t *testing.T) {
	hellos := make(chan *tls.ClientHelloInfo, 4)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(r.Proto))
	}))
	srv.EnableHTTP2 = true
	srv.TLS = &tls.Config{GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
		select {
		case hellos <- h:
		default:
		}
		return nil, nil
	}}
	srv.StartTLS()
	defer srv.Close()

	chromeRootCAs = srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	defer func() { chromeRootCAs = nil }()

	dav := NewWebDAV(srv.URL, "u", "p", 5*time.Second, "")
	req, _ := http.NewRequestWithContext(context.Background(), "GET", srv.URL, nil)
	resp, err := dav.client.Do(req)
	if err != nil {
		t.Fatalf("request over uTLS failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 1 || resp.TLS != nil {
		// resp.TLS is only set for *tls.Conn; a uTLS conn leaves it nil,
		// which also proves the custom dialer was used.
		t.Errorf("proto %s, TLS state %v: want HTTP/1.1 over the uTLS dialer", resp.Proto, resp.TLS != nil)
	}

	h := <-hellos
	if !slices.Equal(h.SupportedProtos, []string{"http/1.1"}) {
		t.Errorf("ALPN = %v, want [http/1.1]", h.SupportedProtos)
	}
	if len(h.CipherSuites) == 0 || !isGREASE(h.CipherSuites[0]) {
		t.Errorf("cipher suites %x: want a leading GREASE value like Chrome", h.CipherSuites)
	}
}

// "go" keeps the standard library handshake.
func TestGoFingerprint(t *testing.T) {
	TLSFingerprint = "go"
	defer func() { TLSFingerprint = "chrome" }()

	dav := NewWebDAV("https://example.invalid", "u", "p", time.Second, "")
	if tr := dav.client.Transport.(*browserTransport).rt.(*http.Transport); tr.DialTLSContext != nil {
		t.Error("DialTLSContext set with TLSFingerprint=go")
	}
}
