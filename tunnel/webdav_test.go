package tunnel

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
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

// fakeMkcolServer mimics just enough WebDAV MKCOL semantics to test
// EnsureBasePath: MKCOL on a path whose parent doesn't exist yet returns
// 409, MKCOL on an already-existing collection returns 405, and MKCOL on a
// path whose parent does exist succeeds with 201 and records it as existing.
type fakeMkcolServer struct {
	mu       sync.Mutex
	existing map[string]bool // "" (root) is always considered to exist
	mkcols   []string        // order MKCOL was called, for asserting recursion order
}

func newFakeMkcolServer() *fakeMkcolServer {
	return &fakeMkcolServer{existing: make(map[string]bool)}
}

func (f *fakeMkcolServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "MKCOL" {
		w.WriteHeader(http.StatusOK)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	p := strings.TrimRight(r.URL.Path, "/")
	f.mkcols = append(f.mkcols, p)

	if f.existing[p] {
		w.WriteHeader(http.StatusMethodNotAllowed) // 405: already exists
		return
	}
	parent := p[:strings.LastIndex(p, "/")]
	if parent != "" && !f.existing[parent] {
		w.WriteHeader(http.StatusConflict) // 409: parent missing
		return
	}
	f.existing[p] = true
	w.WriteHeader(http.StatusCreated)
}

func TestEnsureBasePathCreatesNestedSegmentsInOrder(t *testing.T) {
	fake := newFakeMkcolServer()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	dav := NewWebDAV(srv.URL+"/a/b/c", "user", "pass", 5*time.Second, "")

	if err := dav.EnsureBasePath(context.Background()); err != nil {
		t.Fatalf("EnsureBasePath: %v", err)
	}

	want := []string{"/a", "/a/b", "/a/b/c"}
	fake.mu.Lock()
	got := fake.mkcols
	fake.mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("mkcols = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("mkcols[%d] = %q, want %q (must create shallowest segment first)", i, got[i], want[i])
		}
	}
}

func TestEnsureBasePathNoOpForRootURL(t *testing.T) {
	fake := newFakeMkcolServer()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	dav := NewWebDAV(srv.URL, "user", "pass", 5*time.Second, "")

	if err := dav.EnsureBasePath(context.Background()); err != nil {
		t.Fatalf("EnsureBasePath: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.mkcols) != 0 {
		t.Errorf("mkcols = %v, want none (base URL has no path)", fake.mkcols)
	}
}

func TestEnsureBasePathToleratesAlreadyExisting(t *testing.T) {
	fake := newFakeMkcolServer()
	fake.existing["/testpath"] = true // simulate a folder that already exists
	srv := httptest.NewServer(fake)
	defer srv.Close()

	dav := NewWebDAV(srv.URL+"/testpath", "user", "pass", 5*time.Second, "")

	if err := dav.EnsureBasePath(context.Background()); err != nil {
		t.Fatalf("EnsureBasePath on an already-existing path should not error: %v", err)
	}
}
