package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
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
	return &WebDAV{
		baseURL:  strings.TrimRight(baseURL, "/"),
		login:    login,
		password: password,
		client:   &http.Client{Timeout: timeout, Transport: &cfTransport{rt: transport}},
	}
}

type cfTransport struct {
	rt http.RoundTripper
}

func (t *cfTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Mimic a regular browser to reduce Cloudflare Bot Score.
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
	return t.rt.RoundTrip(req)
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
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
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
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
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
