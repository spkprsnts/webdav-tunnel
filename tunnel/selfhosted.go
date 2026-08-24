package tunnel

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/net/webdav"
)

// SelfHostedBackend describes one embedded WebDAV listener for -mode selfhosted.
type SelfHostedBackend struct {
	ListenAddr string
	StorageDir string
	Login      string
	Password   string
	CertFile   string // optional, built-in TLS
	KeyFile    string
}

// RunSelfHosted starts one embedded WebDAV server per entry in backends. If
// storageOnly is false (the default), it then runs a single tunnel relay
// (RunServer) polling all of them, rotating new sessions across them —
// exactly like an external `-mode server` with a backends: list would, but
// as one process from one config. Blocks indefinitely. Clients connect
// using the printed:
//
//	-mode client -uri <printed-uri> -socks-listen 127.0.0.1:1080
//
// Running several backends from one process (this) is the simple way to
// use self-hosted multi-backend rotation without managing separate
// processes — but they share fate: if this process or its machine goes
// down, every backend goes with it. For resilience against that, run each
// backend as its own storageOnly process (each on its own machine) behind a
// separate external `-mode server -config ...` that lists all of them —
// see docs/config.md#multi-backend-rotation.
//
// If storageOnly is true, no relay is started here at all — the backends
// only serve WebDAV storage, for an external server to poll. Don't point a
// client directly at a storage-only node (via -uri): there's no relay
// there to pick up its sessions. And never point both a storage-only
// node's own relay and an external one at the same storage simultaneously
// — storageOnly exists precisely to prevent that double-relay conflict.
func RunSelfHosted(backends []SelfHostedBackend, proxy *ProxyConfig, timeout time.Duration, encrypt bool, healthListen string, storageOnly bool) {
	if encrypt {
		log.Printf("encryption: enabled (AES-256-GCM, key derived per backend from its own login+password via scrypt)")
	}

	var poolBackends []*Backend
	var refs []BackendRef
	for _, b := range backends {
		dav, publicBase := startEmbeddedWebDAV(b, timeout)

		var key []byte
		if encrypt {
			var err error
			key, err = DeriveKey(b.Login, b.Password)
			if err != nil {
				log.Fatalf("selfhosted: derive encryption key for %s: %v", publicBase, err)
			}
		}

		if storageOnly {
			log.Printf("selfhosted: %s ready (storage-only) — add as a backend:  url: %s  login: %s  password: %s",
				publicBase, publicBase, b.Login, b.Password)
		}

		poolBackends = append(poolBackends, &Backend{Label: publicBase, Dav: dav, EncKey: key})
		refs = append(refs, BackendRef{URL: publicBase, Login: b.Login, Password: b.Password})
	}

	if storageOnly {
		select {} // serve WebDAV forever; no relay loop
	}

	primary, extra := refs[0], refs[1:]
	printClientURI("selfhosted", selfhostedClientURI(primary.URL, primary.Login, primary.Password, encrypt, extra))

	pool := NewBackendPool(poolBackends)
	RunServer(pool, proxy, healthListen)
}

// startEmbeddedWebDAV starts one embedded WebDAV HTTP server for b and
// blocks until it responds. Returns a *WebDAV client that reaches it via
// localhost (used internally by the relay) and its externally-reachable
// base URL (for the printed client URI / storage-only banner — host
// defaults to YOUR_SERVER_IP for a wildcard bind).
func startEmbeddedWebDAV(b SelfHostedBackend, timeout time.Duration) (dav *WebDAV, publicBase string) {
	if err := os.MkdirAll(b.StorageDir, 0o755); err != nil {
		log.Fatalf("selfhosted: cannot create storage dir %q: %v", b.StorageDir, err)
	}

	wdHandler := &webdav.Handler{
		FileSystem: webdav.Dir(b.StorageDir),
		LockSystem: webdav.NewMemLS(),
	}

	// Wrap with atomic PUT handler to prevent concurrent GET seeing a partial write.
	srv := &http.Server{
		Addr:              b.ListenAddr,
		Handler:           basicAuth(b.Login, b.Password, &atomicPUTHandler{storageDir: b.StorageDir, next: wdHandler}),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		var err error
		if b.CertFile != "" && b.KeyFile != "" {
			log.Printf("selfhosted: WebDAV listening (TLS) on %s", b.ListenAddr)
			err = srv.ListenAndServeTLS(b.CertFile, b.KeyFile)
		} else {
			log.Printf("selfhosted: WebDAV listening on %s", b.ListenAddr)
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("selfhosted: WebDAV server: %v", err)
		}
	}()

	localURL := buildLocalURL(b.ListenAddr, b.CertFile != "")
	dav = newWebDAVLoopback(localURL, b.Login, b.Password, timeout) // loopback-only; cert may not cover 127.0.0.1 — see newWebDAVLoopback

	ctx := context.Background()
	var pingErr error
	for i := 0; i < 20; i++ {
		if pingErr = dav.Ping(ctx); pingErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pingErr != nil {
		log.Fatalf("selfhosted: embedded WebDAV at %s never became reachable: %v", localURL, pingErr)
	}
	log.Printf("selfhosted: embedded WebDAV ready at %s", localURL)

	scheme := "http"
	if b.CertFile != "" {
		scheme = "https"
	}
	host, port, _ := net.SplitHostPort(b.ListenAddr)
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "YOUR_SERVER_IP"
	}
	publicBase = fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, port))
	return dav, publicBase
}

// atomicPUTHandler intercepts PUT requests and writes via temp-file + rename,
// so concurrent GETs never observe a partially-written chunk file.
// All other WebDAV methods are forwarded to next unchanged.
type atomicPUTHandler struct {
	storageDir string
	next       http.Handler
}

func (h *atomicPUTHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "PUT" {
		h.next.ServeHTTP(w, r)
		return
	}

	rel := path.Clean("/" + r.URL.Path)
	target := filepath.Join(h.storageDir, filepath.FromSlash(rel))
	dir := filepath.Dir(target)

	tmp, err := os.CreateTemp(dir, ".chunk-")
	if err != nil {
		// Directory probably doesn't exist yet (MKCOL not called). Let
		// webdav.Handler return the proper 409 Conflict.
		h.next.ServeHTTP(w, r)
		return
	}
	tmpName := tmp.Name()

	if _, err = io.Copy(tmp, r.Body); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	tmp.Close()

	if err = os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		http.Error(w, "rename failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// selfhostedClientURI builds the URI for clients connecting over the network.
// Poll settings use network-safe defaults (the server's fast local values,
// auto-tuned for polling its own localhost storage, would be too aggressive
// for a remote client). Chunk/concurrency settings are inherited from the
// current server configuration. extra packs additional backends the same
// way ClientURI does.
func selfhostedClientURI(publicBase, login, password string, enc bool, extra []BackendRef) string {
	u, _ := url.Parse(publicBase)
	switch u.Scheme {
	case "http":
		u.Scheme = "webdav"
	case "https":
		u.Scheme = "webdavs"
	}
	u.User = url.UserPassword(login, password)
	q := url.Values{}
	q.Set("poll-min", "200ms")
	q.Set("poll-max", "500ms")
	q.Set("coalesce", "10ms")
	q.Set("chunk-size", strconv.Itoa(ChunkDataSize))
	q.Set("puts", strconv.Itoa(MaxConcurrentPuts))
	q.Set("read-min", strconv.Itoa(MinReadAheadWindow))
	q.Set("read-max", strconv.Itoa(MaxReadAheadWindow))
	if enc {
		q.Set("enc", "1")
	}
	for _, b := range extra {
		q.Add("backend", backendSubURI(b))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// BackendRef identifies one WebDAV backend for ClientURI's extra backends.
type BackendRef struct {
	URL      string
	Login    string
	Password string
}

// backendSubURI renders a bare webdav://user:pass@host URI (no query, no
// fragment) for embedding as a "backend" query parameter value.
func backendSubURI(b BackendRef) string {
	u, _ := url.Parse(b.URL)
	switch u.Scheme {
	case "http":
		u.Scheme = "webdav"
	case "https":
		u.Scheme = "webdavs"
	}
	u.User = url.UserPassword(b.Login, b.Password)
	return u.String()
}

// ClientURI converts an http/https WebDAV base URL into a webdav:// URI
// with credentials and current tuning settings as query parameters.
// Use this URI as the -uri flag value on the client.
//
// extra holds additional backends for multi-backend rotation: each is
// packed as its own nested webdav://user:pass@host URI in a repeated
// "backend" query parameter. A client that doesn't understand "backend"
// simply ignores it and connects to the primary backend only — the URI
// degrades gracefully instead of failing to parse.
func ClientURI(baseURL, login, password string, enc bool, extra []BackendRef) string {
	u, _ := url.Parse(baseURL)
	switch u.Scheme {
	case "http":
		u.Scheme = "webdav"
	case "https":
		u.Scheme = "webdavs"
	}
	u.User = url.UserPassword(login, password)
	q := url.Values{}
	q.Set("poll-min", MinPollInterval.String())
	q.Set("poll-max", PollInterval.String())
	q.Set("coalesce", CoalesceDelay.String())
	q.Set("chunk-size", strconv.Itoa(ChunkDataSize))
	q.Set("puts", strconv.Itoa(MaxConcurrentPuts))
	q.Set("read-min", strconv.Itoa(MinReadAheadWindow))
	q.Set("read-max", strconv.Itoa(MaxReadAheadWindow))
	if enc {
		q.Set("enc", "1")
	}
	for _, b := range extra {
		q.Add("backend", backendSubURI(b))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func printClientURI(prefix, uri string) {
	log.Printf("%s: ════════════════════════════════════════════════════", prefix)
	log.Printf("%s: client -uri  %s", prefix, uri)
	log.Printf("%s: ════════════════════════════════════════════════════", prefix)
}

// basicAuth wraps next with HTTP Basic authentication.
func basicAuth(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="webdav-tunnel"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// buildLocalURL returns the URL the server uses to reach its own WebDAV.
// A socket bound to a specific non-wildcard address (e.g. 192.168.0.4:8080)
// does not accept connections on 127.0.0.1 — only a genuine wildcard bind
// (0.0.0.0, ::, or an empty host as in ":8080") does, so this must dial the
// address the server actually bound to, not assume loopback works.
func buildLocalURL(listenAddr string, tls bool) string {
	scheme := "http"
	if tls {
		scheme = "https"
	}
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return scheme + "://" + listenAddr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, port))
}
