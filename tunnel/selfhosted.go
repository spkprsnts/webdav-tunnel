package tunnel

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/webdav"
)

// RunSelfHosted starts an embedded WebDAV server on listenAddr, stores all
// session data in storageDir, and then runs the tunnel server loop against it.
// Blocks indefinitely. Clients connect using:
//
//	-mode client -webdav <public-url> -login <login> -password <password>
func RunSelfHosted(listenAddr, storageDir, login, password, certFile, keyFile string, proxy *ProxyConfig, timeout time.Duration) {
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		log.Fatalf("selfhosted: cannot create storage dir %q: %v", storageDir, err)
	}

	wdHandler := &webdav.Handler{
		FileSystem: webdav.Dir(storageDir),
		LockSystem: webdav.NewMemLS(),
	}

	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           basicAuth(login, password, wdHandler),
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
	printClientURI("selfhosted", selfhostedClientURI(publicBase, login, password))

	RunServer(dav, proxy)
}

// selfhostedClientURI builds the URI for clients connecting over the network.
// Poll settings use network-safe defaults (server's fast local values would
// be too aggressive for a remote client). Chunk/concurrency settings are
// inherited from the current server configuration.
func selfhostedClientURI(publicBase, login, password string) string {
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
	u.RawQuery = q.Encode()
	return u.String()
}

// ClientURI converts an http/https WebDAV base URL into a webdav:// URI
// with credentials and current tuning settings as query parameters.
// Use this URI as the -uri flag value on the client.
func ClientURI(baseURL, login, password string) string {
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
