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
	uri := clientURI(listenAddr, login, password, certFile != "")
	log.Printf("selfhosted: ════════════════════════════════════════════════════")
	log.Printf("selfhosted: client -uri  %s", uri)
	log.Printf("selfhosted: ════════════════════════════════════════════════════")

	RunServer(dav, proxy)
}

// clientURI builds the webdav:// URI that clients pass to -uri.
// Tuning parameters are embedded as query params so the client inherits them.
// If the listen address binds to all interfaces, the host is replaced with
// a placeholder the operator fills in with their actual public IP/hostname.
func clientURI(listenAddr, login, password string, tls bool) string {
	scheme := "webdav"
	if tls {
		scheme = "webdavs"
	}
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		host = listenAddr
		port = ""
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "YOUR_SERVER_IP"
	}
	q := url.Values{}
	q.Set("poll-min", MinPollInterval.String())
	q.Set("poll-max", PollInterval.String())
	q.Set("coalesce", CoalesceDelay.String())
	q.Set("chunk-size", strconv.Itoa(ChunkDataSize))
	q.Set("puts", strconv.Itoa(MaxConcurrentPuts))
	q.Set("read-min", strconv.Itoa(MinReadAheadWindow))
	q.Set("read-max", strconv.Itoa(MaxReadAheadWindow))
	u := &url.URL{
		Scheme:   scheme,
		User:     url.UserPassword(login, password),
		Host:     net.JoinHostPort(host, port),
		RawQuery: q.Encode(),
	}
	return u.String()
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
