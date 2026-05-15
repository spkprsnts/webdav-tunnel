package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type WebDAV struct {
	baseURL  string
	login    string
	password string
	client   *http.Client
}

func NewWebDAV(baseURL, login, password string, timeout time.Duration) *WebDAV {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 15 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     false, // Отключаем HTTP/2 (Cloudflare часто "вешает" потоки ботов)
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
	// Маскируемся под обычный браузер, чтобы снизить Bot Score в Cloudflare
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
	if resp.StatusCode == 429 {
		log.Printf("WebDAV 429 rate limited: PUT %s", path)
	}
	io.Copy(io.Discard, resp.Body) // Обязательно вычитываем ответ, чтобы переиспользовать TCP
	resp.Body.Close()
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
	// Запрещаем Cloudflare и прокси-серверам кэшировать 404 ошибки
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	req.Header.Set("Pragma", "no-cache")
	resp, err := w.client.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "connection reset") {
			w.client.CloseIdleConnections()
		}
		return nil, 0, err
	}
	if resp.StatusCode == 429 {
		log.Printf("WebDAV 429 rate limited: GET %s", path)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		io.Copy(io.Discard, resp.Body) // Спасаем пул соединений
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
	if resp.StatusCode == 429 {
		log.Printf("WebDAV 429 rate limited: DELETE %s", path)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
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
	if resp.StatusCode == 429 {
		log.Printf("WebDAV 429 rate limited: MKCOL %s", path)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	// 405 = already exists, 409 = parent missing (treat as ok, Mkcol is best-effort)
	if resp.StatusCode >= 400 && resp.StatusCode != 405 && resp.StatusCode != 409 {
		return fmt.Errorf("MKCOL %s: %s", path, resp.Status)
	}
	return nil
}

func (w *WebDAV) Propfind(ctx context.Context, path string) ([]string, error) {
	// Для директорий добавляем слеш на конце.
	// Иначе Apache вернёт 301 редирект, который Go пройдёт обычным GET-запросом и получит HTML страницу автоиндекса.
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	body := `<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/></D:prop></D:propfind>`
	req, err := http.NewRequestWithContext(ctx, "PROPFIND", w.url(path), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(w.login, w.password)
	req.Header.Set("Depth", "1")
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	req.Header.Set("Pragma", "no-cache")
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 429 {
		log.Printf("WebDAV 429 rate limited: PROPFIND %s", path)
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

// SessionAge возвращает время с последнего heartbeat.
// Возвращает -1 если файл hb не существует (новая сессия).
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

// ListSessions returns session IDs found under tunnel/ directory.
// Возвращает только сессии, у которых есть файл init —
// это означает что клиент завершил Init() и все поддиректории уже созданы.
func (w *WebDAV) ListSessions(ctx context.Context) ([]string, error) {
	hrefs, err := w.Propfind(ctx, "tunnel")
	if err != nil || hrefs == nil {
		return nil, err
	}
	var sessions []string
	for _, href := range hrefs {
		id := lastPathSegment(href)
		if id == "" || id == "tunnel" {
			continue
		}
		_, status, _ := w.Get(ctx, "tunnel/"+id+"/init")
		if status == 200 {
			sessions = append(sessions, id)
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
