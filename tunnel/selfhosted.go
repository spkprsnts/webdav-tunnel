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

// RunSelfHosted starts an embedded WebDAV server on listenAddr, stores all
// session data in storageDir, and then runs the tunnel server loop against it.
// Blocks indefinitely. Clients connect using:
//
//	-mode client -uri <printed-uri> -socks-listen 127.0.0.1:1080
func RunSelfHosted(listenAddr, storageDir, login, password, certFile, keyFile string, proxy *ProxyConfig, timeout time.Duration, encKey []byte) {
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		log.Fatalf("selfhosted: cannot create storage dir %q: %v", storageDir, err)
	}

	wdHandler := &webdav.Handler{
		FileSystem: webdav.Dir(storageDir),
		LockSystem: webdav.NewMemLS(),
	}

	// Wrap with atomic PUT handler to prevent concurrent GET seeing a partial write.
	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           basicAuth(login, password, &atomicPUTHandler{storageDir: storageDir, next: wdHandler}),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		var err error
		if certFile != "" && keyFile != "" {
			log.Printf("selfhosted: WebDAV listening (TLS) on %s", listenAddr)
			err = srv.ListenAndServeTLS(certFile, keyFile)
		} else {
			log.Printf("selfhosted: WebDAV listening on %s", listenAddr)
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("selfhosted: WebDAV server: %v", err)
		}
	}()

	localURL := buildLocalURL(listenAddr, certFile != "")
	dav := NewWebDAV(localURL, login, password, timeout)

	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := dav.Ping(ctx); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("selfhosted: embedded WebDAV ready at %s", localURL)
	scheme := "http"
	if certFile != "" {
		scheme = "https"
	}
	host, port, _ := net.SplitHostPort(listenAddr)
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "YOUR_SERVER_IP"
	}
	publicBase := fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, port))
	printClientURI("selfhosted", selfhostedClientURI(publicBase, login, password, len(encKey) > 0))

	pool := NewBackendPool([]*Backend{{Label: localURL, Dav: dav, EncKey: encKey}})
	RunServer(pool, proxy)
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
// Poll settings use network-safe defaults (server's fast local values would
// be too aggressive for a remote client). Chunk/concurrency settings are
// inherited from the current server configuration.
func selfhostedClientURI(publicBase, login, password string, enc bool) string {
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
func buildLocalURL(listenAddr string, tls bool) string {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		scheme := "http"
		if tls {
			scheme = "https"
		}
		return scheme + "://" + listenAddr
	}
	scheme := "http"
	if tls {
		scheme = "https"
	}
	return fmt.Sprintf("%s://127.0.0.1:%s", scheme, port)
}
