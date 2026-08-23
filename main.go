package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"sync"
	"time"

	"webdav-tunnel/tunnel"
)

// version is set at build time via -ldflags "-X main.version=vX.Y.Z"
// (see .goreleaser.yaml). Left as "dev" for local `go build`.
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	mode := flag.String("mode", "", "client | server | selfhosted")
	configPath := flag.String("config", "", "path to a YAML config file (see docs/config.md); CLI flags override config values")
	uriFlag := flag.String("uri", "", "client connection URI: webdav://user:pass@host:port[?tuning] (replaces -webdav/-login/-password and tuning flags)")
	webdavURL := flag.String("webdav", "", "WebDAV base URL (client and server modes)")
	login := flag.String("login", "", "WebDAV username")
	password := flag.String("password", "", "WebDAV password / app password")
	listen := flag.String("socks-listen", "", "address to listen for SOCKS5 connections (client mode)")
	socksUser := flag.String("socks-user", "", "SOCKS5 proxy username (client mode, optional)")
	socksPass := flag.String("socks-pass", "", "SOCKS5 proxy password (client mode, optional)")
	proxyStr := flag.String("proxy", "", "upstream SOCKS5 proxy for the server: socks5://[user:pass@]host:port")
	timeout := flag.Duration("timeout", 60*time.Second, "HTTP request timeout")
	dnsServer := flag.String("dns", "", "DNS server to resolve WebDAV backend hostnames with, e.g. 1.1.1.1:53 (default: OS resolver). Only affects reaching the backend itself, not SOCKS5-tunneled traffic")
	healthListen := flag.String("health-listen", "", "address:port to serve a JSON health/status endpoint on, e.g. 127.0.0.1:9090 (default: disabled). No authentication — bind to loopback or firewall it")

	// selfhosted mode
	webdavListen := flag.String("webdav-listen", "", "address:port for the embedded WebDAV server (selfhosted mode), e.g. :8080")
	webdavStorage := flag.String("webdav-storage", "webdav-data", "directory for WebDAV session data (selfhosted mode)")
	webdavTLSCert := flag.String("webdav-tls-cert", "", "TLS certificate file (selfhosted mode, optional)")
	webdavTLSKey := flag.String("webdav-tls-key", "", "TLS key file (selfhosted mode, optional)")

	encrypt := flag.Bool("enc", false, "encrypt tunnel data with AES-256-GCM (key derived from each backend's WebDAV password)")
	pollMax := flag.Duration("poll-max", tunnel.PollInterval, "maximum poll interval when idle")
	pollMin := flag.Duration("poll-min", tunnel.MinPollInterval, "starting poll interval (adaptive backoff)")
	coalesce := flag.Duration("coalesce", tunnel.CoalesceDelay, "write coalescing window")
	chunkSize := flag.Int("chunk-size", tunnel.ChunkDataSize, "chunk size in bytes")
	puts := flag.Int("puts", tunnel.MaxConcurrentPuts, "parallel upload limit")
	readAheadMin := flag.Int("read-min", tunnel.MinReadAheadWindow, "minimum concurrent prefetch GETs")
	readAheadMax := flag.Int("read-max", tunnel.MaxReadAheadWindow, "maximum concurrent prefetch GETs")
	flag.Parse()

	if *showVersion {
		fmt.Println("webdav-tunnel " + version)
		return
	}

	// Track which flags the user set explicitly on the command line — used
	// below to decide whether config values, selfhosted defaults, or URI
	// query params should be applied. Config values that get applied are
	// also marked here, so they in turn take priority over selfhosted
	// defaults and -uri params without a second tracking map.
	explicit := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	var cfgBackends []BackendConfig
	if *configPath != "" {
		cfg, err := LoadConfig(*configPath)
		if err != nil {
			log.Fatalf("config: %v", err)
		}
		applyConfig(cfg, explicit, configFlags{
			mode: mode, webdavURL: webdavURL, login: login, password: password,
			listen: listen, socksUser: socksUser, socksPass: socksPass, proxyStr: proxyStr,
			timeout: timeout, encrypt: encrypt, dnsServer: dnsServer, healthListen: healthListen,
			webdavListen: webdavListen, webdavStorage: webdavStorage,
			webdavTLSCert: webdavTLSCert, webdavTLSKey: webdavTLSKey,
			pollMin: pollMin, pollMax: pollMax, coalesce: coalesce,
			chunkSize: chunkSize, puts: puts, readAheadMin: readAheadMin, readAheadMax: readAheadMax,
		})
		cfgBackends = cfg.Backends
	}

	// Selfhosted: WebDAV is local, so use aggressive defaults for unset flags.
	// Selfhosted: server polls its own localhost WebDAV, so faster intervals are safe.
	// Concurrency stays at defaults — more parallel requests risk overwhelming
	// the embedded filesystem-backed handler.
	if *mode == "selfhosted" {
		if !explicit["poll-max"] {
			*pollMax = 200 * time.Millisecond
		}
		if !explicit["poll-min"] {
			*pollMin = 50 * time.Millisecond
		}
		if !explicit["coalesce"] {
			*coalesce = 5 * time.Millisecond
		}
	}

	// Client with -uri: parse credentials and apply tuning from query params
	// for any flag the user did not set explicitly on the command line.
	if *uriFlag != "" {
		var q url.Values
		var extraBackends []BackendConfig
		*webdavURL, *login, *password, q, extraBackends = parseClientURI(*uriFlag)
		if v := parseDurParam(q, "poll-min"); v != nil && !explicit["poll-min"] {
			*pollMin = *v
		}
		if v := parseDurParam(q, "poll-max"); v != nil && !explicit["poll-max"] {
			*pollMax = *v
		}
		if v := parseDurParam(q, "coalesce"); v != nil && !explicit["coalesce"] {
			*coalesce = *v
		}
		if v := parseIntParam(q, "chunk-size"); v != nil && !explicit["chunk-size"] {
			*chunkSize = *v
		}
		if v := parseIntParam(q, "puts"); v != nil && !explicit["puts"] {
			*puts = *v
		}
		if v := parseIntParam(q, "read-min"); v != nil && !explicit["read-min"] {
			*readAheadMin = *v
		}
		if v := parseIntParam(q, "read-max"); v != nil && !explicit["read-max"] {
			*readAheadMax = *v
		}
		if q.Get("enc") == "1" && !explicit["enc"] {
			*encrypt = true
		}
		if len(extraBackends) > 0 {
			cfgBackends = append([]BackendConfig{{URL: *webdavURL, Login: *login, Password: *password}}, extraBackends...)
		}
	}

	tunnel.PollInterval = *pollMax
	tunnel.MinPollInterval = *pollMin
	tunnel.CoalesceDelay = *coalesce
	tunnel.ChunkDataSize = *chunkSize
	tunnel.MaxConcurrentPuts = *puts
	tunnel.MinReadAheadWindow = *readAheadMin
	tunnel.MaxReadAheadWindow = *readAheadMax

	switch *mode {
	case "client":
		if len(cfgBackends) == 0 && *uriFlag == "" {
			requireWebDAVFlags(*webdavURL, *login, *password)
		}
		if *listen == "" {
			log.Fatal("-socks-listen required for client mode")
		}
		pool := buildPool(cfgBackends, *webdavURL, *login, *password, *dnsServer, *timeout, *encrypt)
		if *socksUser != "" {
			log.Printf("SOCKS5 auth enabled for user %q", *socksUser)
		}
		if err := tunnel.RunProxy(context.Background(), pool, *listen, *socksUser, *socksPass, *healthListen); err != nil {
			log.Fatalf("proxy: %v", err)
		}

	case "server":
		if len(cfgBackends) == 0 {
			requireWebDAVFlags(*webdavURL, *login, *password)
		}
		pool := buildPool(cfgBackends, *webdavURL, *login, *password, *dnsServer, *timeout, *encrypt)
		log.Printf("server: ════════════════════════════════════════════════════")
		log.Printf("server: client -uri  %s", serverClientURI(cfgBackends, *webdavURL, *login, *password, *encrypt))
		log.Printf("server: ════════════════════════════════════════════════════")
		tunnel.RunServer(pool, parseProxy(*proxyStr), *healthListen)

	case "selfhosted":
		if *login == "" || *password == "" {
			log.Fatal("required flags: -login, -password")
		}
		if *webdavListen == "" {
			log.Fatal("-webdav-listen required for selfhosted mode (e.g., :8080)")
		}
		var encKey []byte
		if *encrypt {
			var err error
			encKey, err = tunnel.DeriveKey(*login, *password)
			if err != nil {
				log.Fatalf("derive encryption key: %v", err)
			}
			log.Printf("encryption: enabled (AES-256-GCM, key derived from WebDAV login+password via scrypt)")
		}
		tunnel.RunSelfHosted(*webdavListen, *webdavStorage, *login, *password, *webdavTLSCert, *webdavTLSKey, parseProxy(*proxyStr), *timeout, encKey, *healthListen)

	default:
		log.Fatal("-mode must be: client | server | selfhosted")
	}
}

// configFlags bundles the flag variables applyConfig may fill in from YAML.
type configFlags struct {
	mode, webdavURL, login, password            *string
	listen, socksUser, socksPass, proxyStr      *string
	dnsServer, healthListen                     *string
	timeout                                     *time.Duration
	encrypt                                     *bool
	webdavListen, webdavStorage                 *string
	webdavTLSCert, webdavTLSKey                 *string
	pollMin, pollMax, coalesce                  *time.Duration
	chunkSize, puts, readAheadMin, readAheadMax *int
}

// applyConfig fills in any flag not explicitly set on the command line with
// the corresponding value from cfg, marking it explicit so it in turn takes
// priority over selfhosted auto-defaults and -uri query params.
func applyConfig(cfg *Config, explicit map[string]bool, f configFlags) {
	setStr := func(dst *string, name, val string) {
		if val != "" && !explicit[name] {
			*dst = val
			explicit[name] = true
		}
	}
	setStr(f.mode, "mode", cfg.Mode)
	setStr(f.webdavURL, "webdav", cfg.Webdav)
	setStr(f.login, "login", cfg.Login)
	setStr(f.password, "password", cfg.Password)
	setStr(f.listen, "socks-listen", cfg.SocksListen)
	setStr(f.socksUser, "socks-user", cfg.SocksUser)
	setStr(f.socksPass, "socks-pass", cfg.SocksPass)
	setStr(f.proxyStr, "proxy", cfg.Proxy)
	setStr(f.dnsServer, "dns", cfg.DNS)
	setStr(f.healthListen, "health-listen", cfg.HealthListen)
	setStr(f.webdavListen, "webdav-listen", cfg.WebdavListen)
	setStr(f.webdavStorage, "webdav-storage", cfg.WebdavStorage)
	setStr(f.webdavTLSCert, "webdav-tls-cert", cfg.WebdavTLSCert)
	setStr(f.webdavTLSKey, "webdav-tls-key", cfg.WebdavTLSKey)

	if cfg.Enc && !explicit["enc"] {
		*f.encrypt = true
		explicit["enc"] = true
	}
	if cfg.Timeout != nil && !explicit["timeout"] {
		*f.timeout = time.Duration(*cfg.Timeout)
		explicit["timeout"] = true
	}

	setDur := func(dst *time.Duration, name string, val *Duration) {
		if val != nil && !explicit[name] {
			*dst = time.Duration(*val)
			explicit[name] = true
		}
	}
	setInt := func(dst *int, name string, val *int) {
		if val != nil && !explicit[name] {
			*dst = *val
			explicit[name] = true
		}
	}
	setDur(f.pollMin, "poll-min", cfg.Tuning.PollMin)
	setDur(f.pollMax, "poll-max", cfg.Tuning.PollMax)
	setDur(f.coalesce, "coalesce", cfg.Tuning.Coalesce)
	setInt(f.chunkSize, "chunk-size", cfg.Tuning.ChunkSize)
	setInt(f.puts, "puts", cfg.Tuning.Puts)
	setInt(f.readAheadMin, "read-min", cfg.Tuning.ReadMin)
	setInt(f.readAheadMax, "read-max", cfg.Tuning.ReadMax)
}

// parseClientURI parses the primary backend (userinfo+host) and tuning
// query params from a -uri value, exactly as before. Any repeated
// "backend" query parameter holds an additional nested webdav://user:pass@host
// URI for multi-backend rotation (see tunnel.ClientURI) — these are
// returned as extra.
func parseClientURI(rawURI string) (webdavURL, login, password string, query url.Values, extra []BackendConfig) {
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

	for _, raw := range query["backend"] {
		bu, err := url.Parse(raw)
		if err != nil {
			log.Fatalf("invalid backend= entry in -uri: %v", err)
		}
		switch bu.Scheme {
		case "webdav":
			bu.Scheme = "http"
		case "webdavs":
			bu.Scheme = "https"
		default:
			log.Fatalf("backend= entry must use webdav:// or webdavs://")
		}
		var bLogin, bPassword string
		if bu.User != nil {
			bLogin = bu.User.Username()
			bPassword, _ = bu.User.Password()
			bu.User = nil
		}
		extra = append(extra, BackendConfig{URL: bu.String(), Login: bLogin, Password: bPassword})
	}
	return
}

// serverClientURI builds the -uri banner the server prints for the client
// to copy. With a single backend this is unchanged; with multiple backends
// (from -config's backends: list) it packs all of them into one URI via
// tunnel.ClientURI's repeated backend= query parameter.
func serverClientURI(cfgBackends []BackendConfig, webdavURL, login, password string, enc bool) string {
	if len(cfgBackends) == 0 {
		return tunnel.ClientURI(webdavURL, login, password, enc, nil)
	}
	primary := cfgBackends[0]
	extra := make([]tunnel.BackendRef, 0, len(cfgBackends)-1)
	for _, b := range cfgBackends[1:] {
		extra = append(extra, tunnel.BackendRef{URL: b.URL, Login: b.Login, Password: b.Password})
	}
	return tunnel.ClientURI(primary.URL, primary.Login, primary.Password, enc, extra)
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
		log.Fatal("required flags: -webdav, -login, -password (or -config with a backends: list)")
	}
}

// newBackend builds one pool backend, deriving its encryption key (if any)
// from its own WebDAV login and password.
func newBackend(label, rawURL, login, password, dnsServer string, timeout time.Duration, encrypt bool) *tunnel.Backend {
	var key []byte
	if encrypt {
		var err error
		key, err = tunnel.DeriveKey(login, password)
		if err != nil {
			log.Fatalf("derive encryption key for backend %s: %v", label, err)
		}
	}
	return &tunnel.Backend{
		Label:  label,
		Dav:    tunnel.NewWebDAV(rawURL, login, password, timeout, dnsServer),
		EncKey: key,
	}
}

// buildPool builds the backend pool from a multi-backend config list if
// present, otherwise falls back to the single legacy -webdav/-login/-password
// (or -uri-derived) backend. It pings every backend in parallel and only
// fails hard if none of them are reachable — a dead backend shouldn't block
// startup when others are available.
func buildPool(backendsCfg []BackendConfig, webdavURL, login, password, dnsServer string, timeout time.Duration, encrypt bool) *tunnel.BackendPool {
	var backends []*tunnel.Backend
	if len(backendsCfg) > 0 {
		for i, bc := range backendsCfg {
			label := bc.URL
			if label == "" {
				label = fmt.Sprintf("backend-%d", i+1)
			}
			backends = append(backends, newBackend(label, bc.URL, bc.Login, bc.Password, dnsServer, timeout, encrypt))
		}
	} else {
		backends = append(backends, newBackend(webdavURL, webdavURL, login, password, dnsServer, timeout, encrypt))
	}

	pool := tunnel.NewBackendPool(backends)
	pingBackends(pool)
	return pool
}

// pingBackends checks connectivity and authentication for every backend in
// parallel, logging the result for each. It only aborts the program if every
// backend is unreachable.
func pingBackends(pool *tunnel.BackendPool) {
	backends := pool.All()
	errs := make([]error, len(backends))
	var wg sync.WaitGroup
	for i, b := range backends {
		wg.Add(1)
		go func(i int, b *tunnel.Backend) {
			defer wg.Done()
			start := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := b.Dav.Ping(ctx); err != nil {
				log.Printf("WebDAV backend %s: connection failed: %v", b.Label, err)
				errs[i] = err
				return
			}
			log.Printf("WebDAV backend %s: OK (%dms)", b.Label, time.Since(start).Milliseconds())
		}(i, b)
	}
	wg.Wait()

	for _, err := range errs {
		if err == nil {
			return // at least one backend is reachable
		}
	}
	log.Fatalf("all %d WebDAV backend(s) unreachable", len(backends))
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
