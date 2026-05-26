package main

import (
	"context"
	"flag"
	"log"
	"net/url"
	"strconv"
	"time"

	"webdav-tunnel/tunnel"
)

func main() {
	mode      := flag.String("mode", "", "client | server | selfhosted")
	uriFlag   := flag.String("uri", "", "client connection URI: webdav://user:pass@host:port[?tuning] (replaces -webdav/-login/-password and tuning flags)")
	webdavURL := flag.String("webdav", "", "WebDAV base URL (client and server modes)")
	login     := flag.String("login", "", "WebDAV username")
	password  := flag.String("password", "", "WebDAV password / app password")
	listen    := flag.String("socks-listen", "", "address to listen for SOCKS5 connections (client mode)")
	socksUser := flag.String("socks-user", "", "SOCKS5 proxy username (client mode, optional)")
	socksPass := flag.String("socks-pass", "", "SOCKS5 proxy password (client mode, optional)")
	proxyStr  := flag.String("proxy", "", "upstream SOCKS5 proxy for the server: socks5://[user:pass@]host:port")
	timeout   := flag.Duration("timeout", 60*time.Second, "HTTP request timeout")

	// selfhosted mode
	webdavListen  := flag.String("webdav-listen", "", "address:port for the embedded WebDAV server (selfhosted mode), e.g. :8080")
	webdavStorage := flag.String("webdav-storage", "webdav-data", "directory for WebDAV session data (selfhosted mode)")
	webdavTLSCert := flag.String("webdav-tls-cert", "", "TLS certificate file (selfhosted mode, optional)")
	webdavTLSKey  := flag.String("webdav-tls-key", "", "TLS key file (selfhosted mode, optional)")

	pollMax      := flag.Duration("poll-max", tunnel.PollInterval, "maximum poll interval when idle")
	pollMin      := flag.Duration("poll-min", tunnel.MinPollInterval, "starting poll interval (adaptive backoff)")
	coalesce     := flag.Duration("coalesce", tunnel.CoalesceDelay, "write coalescing window")
	chunkSize    := flag.Int("chunk-size", tunnel.ChunkDataSize, "chunk size in bytes")
	puts         := flag.Int("puts", tunnel.MaxConcurrentPuts, "parallel upload limit")
	readAheadMin := flag.Int("read-min", tunnel.MinReadAheadWindow, "minimum concurrent prefetch GETs")
	readAheadMax := flag.Int("read-max", tunnel.MaxReadAheadWindow, "maximum concurrent prefetch GETs")
	flag.Parse()

	// Track which tuning flags the user set explicitly — used below to decide
	// whether selfhosted defaults or URI query params should be applied.
	explicit := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	// Selfhosted: WebDAV is local, so use aggressive defaults for unset flags.
	if *mode == "selfhosted" {
		if !explicit["poll-max"]  { *pollMax      = 100 * time.Millisecond }
		if !explicit["poll-min"]  { *pollMin      = 50 * time.Millisecond }
		if !explicit["coalesce"]  { *coalesce     = 5 * time.Millisecond }
		if !explicit["puts"]      { *puts         = 16 }
		if !explicit["read-max"]  { *readAheadMax = 16 }
	}

	// Client with -uri: parse credentials and apply tuning from query params
	// for any flag the user did not set explicitly on the command line.
	if *uriFlag != "" {
		var q url.Values
		*webdavURL, *login, *password, q = parseClientURI(*uriFlag)
		if v := parseDurParam(q, "poll-min");  v != nil && !explicit["poll-min"]   { *pollMin      = *v }
		if v := parseDurParam(q, "poll-max");  v != nil && !explicit["poll-max"]   { *pollMax      = *v }
		if v := parseDurParam(q, "coalesce");  v != nil && !explicit["coalesce"]   { *coalesce     = *v }
		if v := parseIntParam(q, "chunk-size"); v != nil && !explicit["chunk-size"] { *chunkSize   = *v }
		if v := parseIntParam(q, "puts");      v != nil && !explicit["puts"]        { *puts        = *v }
		if v := parseIntParam(q, "read-min");  v != nil && !explicit["read-min"]    { *readAheadMin = *v }
		if v := parseIntParam(q, "read-max");  v != nil && !explicit["read-max"]    { *readAheadMax = *v }
	}

	tunnel.PollInterval       = *pollMax
	tunnel.MinPollInterval    = *pollMin
	tunnel.CoalesceDelay      = *coalesce
	tunnel.ChunkDataSize      = *chunkSize
	tunnel.MaxConcurrentPuts  = *puts
	tunnel.MinReadAheadWindow = *readAheadMin
	tunnel.MaxReadAheadWindow = *readAheadMax

	switch *mode {
	case "client":
		if *uriFlag == "" {
			requireWebDAVFlags(*webdavURL, *login, *password)
		}
		if *listen == "" {
			log.Fatal("-socks-listen required for client mode")
		}
		dav := dialWebDAV(*webdavURL, *login, *password, *timeout)
		if *socksUser != "" {
			log.Printf("SOCKS5 auth enabled for user %q", *socksUser)
		}
		if err := tunnel.RunProxy(context.Background(), dav, *listen, *socksUser, *socksPass); err != nil {
			log.Fatalf("proxy: %v", err)
		}

	case "server":
		requireWebDAVFlags(*webdavURL, *login, *password)
		dav := dialWebDAV(*webdavURL, *login, *password, *timeout)
		tunnel.RunServer(dav, parseProxy(*proxyStr))

	case "selfhosted":
		if *login == "" || *password == "" {
			log.Fatal("required flags: -login, -password")
		}
		if *webdavListen == "" {
			log.Fatal("-webdav-listen required for selfhosted mode (e.g., :8080)")
		}
		tunnel.RunSelfHosted(*webdavListen, *webdavStorage, *login, *password, *webdavTLSCert, *webdavTLSKey, parseProxy(*proxyStr), *timeout)

	default:
		log.Fatal("-mode must be: client | server | selfhosted")
	}
}

func parseClientURI(rawURI string) (webdavURL, login, password string, query url.Values) {
	u, err := url.Parse(rawURI)
	if err != nil {
		log.Fatalf("invalid -uri %q: %v", rawURI, err)
	}
	switch u.Scheme {
	case "webdav":
		u.Scheme = "http"
	case "webdavs":
		u.Scheme = "https"
	default:
		log.Fatalf("-uri scheme must be webdav:// or webdavs://")
	}
	if u.User != nil {
		login = u.User.Username()
		password, _ = u.User.Password()
		u.User = nil
	}
	query = u.Query()
	u.RawQuery = ""
	webdavURL = u.String()
	return
}

func parseDurParam(q url.Values, key string) *time.Duration {
	v := q.Get(key)
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return nil
	}
	return &d
}

func parseIntParam(q url.Values, key string) *int {
	v := q.Get(key)
	if v == "" {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return nil
	}
	return &n
}

func requireWebDAVFlags(webdavURL, login, password string) {
	if webdavURL == "" || login == "" || password == "" {
		log.Fatal("required flags: -webdav, -login, -password")
	}
}

func dialWebDAV(rawURL, login, password string, timeout time.Duration) *tunnel.WebDAV {
	dav := tunnel.NewWebDAV(rawURL, login, password, timeout)
	log.Printf("WebDAV: connecting to %s...", rawURL)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := dav.Ping(ctx); err != nil {
		log.Fatalf("WebDAV: connection failed: %v", err)
	}
	log.Printf("WebDAV: OK (%dms)", time.Since(start).Milliseconds())
	return dav
}

func parseProxy(proxyStr string) *tunnel.ProxyConfig {
	if proxyStr == "" {
		return nil
	}
	u, err := url.Parse(proxyStr)
	if err != nil || u.Scheme != "socks5" || u.Host == "" {
		log.Fatal("-proxy must be socks5://[user:pass@]host:port")
	}
	user, pass := "", ""
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	return tunnel.NewProxyConfig(u.Host, user, pass)
}
