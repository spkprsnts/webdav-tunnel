package tunnel

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	utls "github.com/refraction-networking/utls"
)

type rateLimitError struct {
	wait time.Duration
}

func (e *rateLimitError) Error() string {
	return fmt.Sprintf("rate limited (retry after %v)", e.wait)
}

func parseRetryAfter(h http.Header) time.Duration {
	ra := h.Get("Retry-After")
	if ra == "" {
		return 5 * time.Second
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(ra)); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(ra); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 5 * time.Second
}

// TLSFingerprint selects the TLS ClientHello sent to HTTPS WebDAV backends:
//
//	"chrome" (default) — Chrome 133 via uTLS, matching the User-Agent header
//	"go"               — the standard library's crypto/tls
//
// A Go ClientHello next to a Chrome User-Agent is an easy signature for DPI
// to spot, so "go" is only an escape hatch for servers that reject the
// Chrome handshake.
var TLSFingerprint = "chrome"

// chromeRootCAs overrides the system roots for the uTLS handshake (tests only).
var chromeRootCAs *x509.CertPool

// chromeUA and chromeSecCHUA must match the Chrome version of the uTLS
// ClientHello.
const (
	chromeUA      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
	chromeSecCHUA = `"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"`
)

type WebDAV struct {
	baseURL  string
	login    string
	password string
	client   *http.Client
}

// NewWebDAV creates a WebDAV client. dnsServer, if non-empty (e.g.
// "1.1.1.1:53"), overrides the OS resolver for looking up baseURL's
// hostname — useful when the client's default DNS is blocked, filtered, or
// otherwise cannot resolve the WebDAV backend. A missing port defaults to
// 53. This only affects resolving the backend itself; it has no effect on
// how the SOCKS5-tunneled traffic's destinations are resolved (that always
// happens server-side, in dialTarget).
func NewWebDAV(baseURL, login, password string, timeout time.Duration, dnsServer string) *WebDAV {
	return newWebDAV(baseURL, login, password, timeout, dnsServer, false)
}

// newWebDAVLoopback is like NewWebDAV but skips TLS certificate
// verification. Only for dialing an embedded selfhosted backend on
// 127.0.0.1: the certificate there is chosen for real remote clients (e.g.
// a domain cert with no 127.0.0.1 SAN) and this connection never leaves the
// host, so there's no MITM to defend against — the socket only accepts
// connections this same machine's kernel routes internally.
func newWebDAVLoopback(baseURL, login, password string, timeout time.Duration) *WebDAV {
	return newWebDAV(baseURL, login, password, timeout, "", true)
}

func newWebDAV(baseURL, login, password string, timeout time.Duration, dnsServer string, insecureSkipVerify bool) *WebDAV {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 15 * time.Second,
	}
	if dnsServer != "" {
		if _, _, err := net.SplitHostPort(dnsServer); err != nil {
			dnsServer = net.JoinHostPort(dnsServer, "53")
		}
		dialer.Resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, dnsServer)
			},
		}
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     false, // HTTP/2 disabled: some cloud providers throttle or fingerprint bot HTTP/2 traffic
		TLSNextProto:          make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		MaxIdleConnsPerHost:   32,
		MaxConnsPerHost:       32,
		IdleConnTimeout:       10 * time.Second,
	}
	if insecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	} else if TLSFingerprint == "chrome" {
		transport.DialTLSContext = chromeDialTLS(dialer, 15*time.Second)
	}
	var origin string
	if u, err := url.Parse(baseURL); err == nil {
		origin = u.Scheme + "://" + u.Host
	}
	return &WebDAV{
		baseURL:  strings.TrimRight(baseURL, "/"),
		login:    login,
		password: password,
		client:   &http.Client{Timeout: timeout, Transport: &browserTransport{rt: transport, origin: origin}},
	}
}

// browserTransport dresses every request as a same-origin fetch() from
// Chrome 133 on Windows — the headers that go with the Chrome User-Agent and
// ClientHello, to keep the Cloudflare Bot Score and similar checks low — and
// decodes the response encodings its Accept-Encoding advertises.
//
// Header order still differs from Chrome's: net/http writes Host and
// User-Agent first, then the rest sorted. Only the WebDAV server sees it
// behind TLS.
type browserTransport struct {
	rt     http.RoundTripper
	origin string // scheme://host of the WebDAV base URL
}

func (t *browserTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context()) // a RoundTripper must not modify its request
	h := req.Header
	h.Set("Connection", "keep-alive")
	h.Set("User-Agent", chromeUA)
	// Chrome sends client hints in lower case; assigning the map directly
	// keeps net/http from canonicalizing them to Sec-Ch-Ua.
	h["sec-ch-ua"] = []string{chromeSecCHUA}
	h["sec-ch-ua-mobile"] = []string{"?0"}
	h["sec-ch-ua-platform"] = []string{`"Windows"`}
	if h.Get("Accept") == "" {
		h.Set("Accept", "*/*")
	}
	if req.Method != http.MethodGet && req.Method != http.MethodHead && t.origin != "" {
		h.Set("Origin", t.origin)
	}
	h.Set("Sec-Fetch-Site", "same-origin")
	h.Set("Sec-Fetch-Mode", "cors")
	h.Set("Sec-Fetch-Dest", "empty")
	if t.origin != "" {
		h.Set("Referer", t.origin+"/")
	}
	h.Set("Accept-Encoding", "gzip, deflate, br, zstd")
	h.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := t.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	// Setting Accept-Encoding ourselves turns off net/http's transparent
	// gzip, so decode here.
	if enc := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))); enc != "" && enc != "identity" {
		resp.Body = &decodingBody{body: resp.Body, enc: enc}
		resp.Header.Del("Content-Encoding")
		resp.Header.Del("Content-Length")
		resp.ContentLength = -1
		resp.Uncompressed = true
	}
	return resp, nil
}

// decodingBody decodes a Content-Encoding lazily, on the first Read, so that
// empty bodies (204, 304, HEAD) never touch the decoder.
type decodingBody struct {
	body     io.ReadCloser
	enc      string
	r        io.Reader
	closeDec func()
	err      error
}

func (b *decodingBody) Read(p []byte) (int, error) {
	if b.r == nil && b.err == nil {
		b.r, b.closeDec, b.err = newBodyDecoder(b.enc, b.body)
	}
	if b.err != nil {
		return 0, b.err
	}
	return b.r.Read(p)
}

func (b *decodingBody) Close() error {
	if b.closeDec != nil {
		b.closeDec()
	}
	return b.body.Close()
}

func newBodyDecoder(enc string, r io.Reader) (io.Reader, func(), error) {
	switch enc {
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, nil, err
		}
		return zr, func() { zr.Close() }, nil
	case "deflate":
		// HTTP "deflate" is zlib-wrapped (RFC 9110), but some servers send
		// raw DEFLATE; browsers accept both, so sniff the zlib header.
		br := bufio.NewReader(r)
		if hdr, err := br.Peek(2); err == nil && hdr[0]&0x0f == 8 && (uint16(hdr[0])<<8|uint16(hdr[1]))%31 == 0 {
			zr, err := zlib.NewReader(br)
			if err != nil {
				return nil, nil, err
			}
			return zr, func() { zr.Close() }, nil
		}
		fr := flate.NewReader(br)
		return fr, func() { fr.Close() }, nil
	case "br":
		return brotli.NewReader(r), nil, nil
	case "zstd":
		zr, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		if err != nil {
			return nil, nil, err
		}
		return zr, zr.Close, nil
	default:
		return nil, nil, fmt.Errorf("unsupported Content-Encoding %q", enc)
	}
}

// chromeDialTLS returns a DialTLSContext that performs a Chrome 133 TLS
// handshake via uTLS.
//
// Chrome offers h2 in ALPN, but the transport only speaks HTTP/1.1 (and Go's
// HTTP/2 frames would give the client away anyway), so ALPN is narrowed to
// http/1.1. The rest of the ClientHello — cipher suites, extension set and
// order shuffling, GREASE, post-quantum key share — stays Chrome's.
func chromeDialTLS(dialer *net.Dialer, handshakeTimeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		// A fresh spec per connection: ApplyPreset mutates it, and GREASE
		// values and extension order are randomized per spec.
		spec, err := utls.UTLSIdToSpec(utls.HelloChrome_133)
		if err != nil {
			return nil, err
		}
		for _, ext := range spec.Extensions {
			if alpn, ok := ext.(*utls.ALPNExtension); ok {
				alpn.AlpnProtocols = []string{"http/1.1"}
			}
		}

		raw, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		conn := utls.UClient(raw, &utls.Config{ServerName: host, RootCAs: chromeRootCAs}, utls.HelloCustom)
		if err := conn.ApplyPreset(&spec); err != nil {
			raw.Close()
			return nil, err
		}
		hsCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
		defer cancel()
		if err := conn.HandshakeContext(hsCtx); err != nil {
			raw.Close()
			return nil, err
		}
		return conn, nil
	}
}

func (w *WebDAV) url(path string) string {
	if path == "" {
		return w.baseURL
	}
	return w.baseURL + "/" + strings.TrimLeft(path, "/")
}

func (w *WebDAV) Put(ctx context.Context, path string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", w.url(path), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.SetBasicAuth(w.login, w.password)
	req.ContentLength = int64(len(data))
	resp, err := w.client.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "connection reset") {
			w.client.CloseIdleConnections()
		}
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == 429 {
		return &rateLimitError{wait: parseRetryAfter(resp.Header)}
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("PUT %s: %s", path, resp.Status)
	}
	return nil
}

func (w *WebDAV) Get(ctx context.Context, path string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", w.url(path), nil)
	if err != nil {
		return nil, 0, err
	}
	req.SetBasicAuth(w.login, w.password)
	// Prevent Cloudflare and proxy caches from serving stale 404s.
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	resp, err := w.client.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "connection reset") {
			w.client.CloseIdleConnections()
		}
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 {
		io.Copy(io.Discard, resp.Body)
		return nil, 429, &rateLimitError{wait: parseRetryAfter(resp.Header)}
	}
	if resp.StatusCode >= 400 {
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == 404 {
			return nil, 404, nil
		}
		return nil, resp.StatusCode, fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	return data, resp.StatusCode, err
}

func (w *WebDAV) Delete(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", w.url(path), nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(w.login, w.password)
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == 429 {
		return &rateLimitError{wait: parseRetryAfter(resp.Header)}
	}
	if resp.StatusCode >= 400 && resp.StatusCode != 404 {
		return fmt.Errorf("DELETE %s: %s", path, resp.Status)
	}
	return nil
}

func (w *WebDAV) Mkcol(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, "MKCOL", w.url(path), nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(w.login, w.password)
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == 429 {
		return &rateLimitError{wait: parseRetryAfter(resp.Header)}
	}
	// 405 = already exists, 409 = parent missing (treat as ok, Mkcol is best-effort)
	if resp.StatusCode >= 400 && resp.StatusCode != 405 && resp.StatusCode != 409 {
		return fmt.Errorf("MKCOL %s: %s", path, resp.Status)
	}
	return nil
}

// EnsureBasePath creates every path segment of the WebDAV base URL that
// doesn't exist yet, one level at a time (e.g. for a base URL ending in
// /a/b/c, it MKCOLs /a, then /a/b, then /a/b/c). Without this, a base URL
// pointing at a not-yet-existing folder (e.g. a fresh backend configured as
// https://example.com/testpath) would 409 forever on every operation under
// it — Mkcol alone can't fix that since it only creates one level and
// treats "parent missing" as non-fatal. Safe to call even when the base
// path already exists (MKCOL on an existing collection is a no-op 405, not
// an error).
func (w *WebDAV) EnsureBasePath(ctx context.Context) error {
	u, err := url.Parse(w.baseURL)
	if err != nil {
		return fmt.Errorf("parse base URL %q: %w", w.baseURL, err)
	}
	trimmed := strings.Trim(u.Path, "/")
	if trimmed == "" {
		return nil // no path prefix — nothing to create
	}
	root := u.Scheme + "://" + u.Host
	path := ""
	for _, seg := range strings.Split(trimmed, "/") {
		path += "/" + seg
		absURL := root + path
		if err := w.mkcolAbs(ctx, absURL); err != nil {
			return fmt.Errorf("mkcol %s: %w", absURL, err)
		}
	}
	return nil
}

// mkcolAbs issues MKCOL against an absolute URL rather than one relative to
// w.baseURL. Unlike Mkcol, a 409 here is treated as a real failure: callers
// create one path segment at a time, so by the time a segment is created
// its parent is already known to exist — a 409 means something else is
// wrong (e.g. permissions), not a normal "create the parent first" case.
func (w *WebDAV) mkcolAbs(ctx context.Context, absURL string) error {
	req, err := http.NewRequestWithContext(ctx, "MKCOL", absURL, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(w.login, w.password)
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == 429 {
		return &rateLimitError{wait: parseRetryAfter(resp.Header)}
	}
	if resp.StatusCode >= 400 && resp.StatusCode != 405 { // 405 = already exists
		return fmt.Errorf("%s", resp.Status)
	}
	return nil
}

func (w *WebDAV) Propfind(ctx context.Context, path string, depth string) ([]string, error) {
	// Trailing slash required for directories: without it Apache returns a 301
	// that Go follows with a plain GET, receiving an HTML index page instead.
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	body := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/></D:prop></D:propfind>`
	req, err := http.NewRequestWithContext(ctx, "PROPFIND", w.url(path), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(w.login, w.password)
	req.Header.Set("Depth", depth)
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 429 {
		io.Copy(io.Discard, resp.Body)
		return nil, &rateLimitError{wait: parseRetryAfter(resp.Header)}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == 404 {
			return nil, nil
		}
		return nil, fmt.Errorf("PROPFIND %s: %s", path, resp.Status)
	}

	var ms struct {
		XMLName   xml.Name `xml:"multistatus"`
		Responses []struct {
			Href string `xml:"href"`
		} `xml:"response"`
	}
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ms.Responses))
	for _, r := range ms.Responses {
		out = append(out, r.Href)
	}
	return out, nil
}

// SessionAge returns the time elapsed since the last heartbeat.
// Returns -1 if the hb file does not exist (new session).
func (w *WebDAV) SessionAge(ctx context.Context, sid string) time.Duration {
	data, status, _ := w.Get(ctx, "tunnel/"+sid+"/hb")
	if status != 200 || len(data) == 0 {
		return -1
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return -1
	}
	return time.Since(time.Unix(ts, 0))
}

// ListSessions returns session IDs found under the tunnel/ directory.
// Only sessions with an init file are returned — that file signals the client
// has finished Init() and all subdirectories are ready.
func (w *WebDAV) ListSessions(ctx context.Context) ([]string, error) {
	hrefs, err := w.Propfind(ctx, "tunnel", "1")
	if err != nil || hrefs == nil {
		return nil, err
	}
	var candidates []string
	for _, href := range hrefs {
		if id := lastPathSegment(href); id != "" && id != "tunnel" {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	// Check init files in parallel.
	type result struct {
		id string
		ok bool
	}
	ch := make(chan result, len(candidates))
	for _, id := range candidates {
		go func(sid string) {
			_, status, _ := w.Get(ctx, "tunnel/"+sid+"/init")
			ch <- result{sid, status == 200}
		}(id)
	}
	var sessions []string
	for range candidates {
		if r := <-ch; r.ok {
			sessions = append(sessions, r.id)
		}
	}
	return sessions, nil
}

// Ping checks WebDAV connectivity and authentication via OPTIONS request.
func (w *WebDAV) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "OPTIONS", w.baseURL+"/", nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(w.login, w.password)
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == 401 {
		return fmt.Errorf("authentication failed (401 Unauthorized)")
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("server returned %s", resp.Status)
	}
	return nil
}

func lastPathSegment(href string) string {
	s := strings.TrimRight(href, "/")
	idx := strings.LastIndex(s, "/")
	if idx < 0 {
		return s
	}
	return s[idx+1:]
}
