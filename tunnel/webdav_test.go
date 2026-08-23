package tunnel

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startFakeDNS starts a minimal UDP DNS server that answers every A query
// with resolveTo. It's just enough of RFC 1035 for Go's pure-Go resolver
// (net.Resolver{PreferGo: true}) to accept the response.
func startFakeDNS(t *testing.T, resolveTo net.IP) (addr string) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake DNS: %v", err)
	}
	t.Cleanup(func() { pc.Close() })

	go func() {
		buf := make([]byte, 512)
		for {
			n, src, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			query := append([]byte(nil), buf[:n]...)
			resp, ok := buildDNSResponse(query, resolveTo)
			if !ok {
				continue
			}
			pc.WriteTo(resp, src)
		}
	}()

	return pc.LocalAddr().String()
}

// buildDNSResponse builds a minimal A-record response for a single-question
// query by echoing the question section back and appending one answer.
func buildDNSResponse(query []byte, ip net.IP) ([]byte, bool) {
	if len(query) < 12 {
		return nil, false
	}
	// Walk the QNAME to find where the question section ends.
	off := 12
	for {
		if off >= len(query) {
			return nil, false
		}
		l := int(query[off])
		if l == 0 {
			off++
			break
		}
		off += 1 + l
	}
	off += 4 // QTYPE + QCLASS
	if off > len(query) {
		return nil, false
	}
	question := query[12:off]

	resp := make([]byte, 0, 12+len(question)+16)
	resp = append(resp, query[0], query[1]) // ID
	resp = append(resp, 0x81, 0x80)         // standard response, no error
	resp = append(resp, 0x00, 0x01)         // QDCOUNT=1
	resp = append(resp, 0x00, 0x01)         // ANCOUNT=1
	resp = append(resp, 0x00, 0x00)         // NSCOUNT=0
	resp = append(resp, 0x00, 0x00)         // ARCOUNT=0
	resp = append(resp, question...)

	ip4 := ip.To4()
	resp = append(resp, 0xC0, 0x0C)             // pointer to question name
	resp = append(resp, 0x00, 0x01)             // TYPE=A
	resp = append(resp, 0x00, 0x01)             // CLASS=IN
	resp = append(resp, 0x00, 0x00, 0x00, 0x3C) // TTL=60
	resp = append(resp, 0x00, 0x04)             // RDLENGTH=4
	resp = append(resp, ip4...)

	return resp, true
}

// TestNewWebDAVCustomDNSServer proves the dnsServer parameter actually
// routes hostname resolution through the given server, rather than just
// being stored and ignored: a made-up .invalid hostname (RFC 2606 — never
// resolves in real DNS) is only reachable if our fake DNS server answered
// the lookup.
func TestNewWebDAVCustomDNSServer(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	backendPort := strings.TrimPrefix(backend.URL, "http://127.0.0.1:")
	if _, err := strconv.Atoi(backendPort); err != nil {
		t.Fatalf("could not extract port from %q", backend.URL)
	}

	fakeDNSAddr := startFakeDNS(t, net.ParseIP("127.0.0.1"))

	baseURL := "http://totally-fake-webdav-tunnel-test.invalid:" + backendPort
	dav := NewWebDAV(baseURL, "user", "pass", 5*time.Second, fakeDNSAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dav.Ping(ctx); err != nil {
		t.Fatalf("Ping with custom DNS server failed: %v (the .invalid hostname should have resolved via the fake DNS server to 127.0.0.1)", err)
	}
}

func TestNewWebDAVDNSServerPortDefaultedTo53(t *testing.T) {
	// A dnsServer value without a port should not panic and should still
	// build a usable client (port defaulting is exercised, not asserted
	// beyond "doesn't crash" since a real :53 lookup needs network access).
	dav := NewWebDAV("http://127.0.0.1:1", "u", "p", time.Second, "127.0.0.1")
	if dav == nil {
		t.Fatal("NewWebDAV returned nil")
	}
}
