package tunnel

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

// captureRawRequest serves one request on a raw socket and returns its header
// block exactly as sent — http.Server would canonicalize the names.
func captureRawRequest(t *testing.T, do func(dav *WebDAV)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		var hdr strings.Builder
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			hdr.WriteString(line)
			if line == "\r\n" {
				break
			}
		}
		got <- hdr.String()
		c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
	}()
	do(NewWebDAV("http://"+ln.Addr().String()+"/dav", "u", "p", 5*time.Second, ""))
	select {
	case h := <-got:
		return h
	case <-time.After(5 * time.Second):
		t.Fatal("no request captured")
		return ""
	}
}

func TestBrowserHeaders(t *testing.T) {
	put := captureRawRequest(t, func(dav *WebDAV) { dav.Put(context.Background(), "x", []byte("data")) })
	origin := "http://" + strings.Fields(strings.SplitN(put, "Host: ", 2)[1])[0]
	for _, want := range []string{
		"User-Agent: " + chromeUA,
		"sec-ch-ua: " + chromeSecCHUA, // lower case, as Chrome sends it
		"sec-ch-ua-mobile: ?0",
		`sec-ch-ua-platform: "Windows"`,
		"Accept: */*",
		"Accept-Encoding: gzip, deflate, br, zstd",
		"Sec-Fetch-Mode: cors",
		"Origin: " + origin,
		"Referer: " + origin + "/",
		"Connection: keep-alive",
	} {
		if !strings.Contains(put, "\r\n"+want+"\r\n") {
			t.Errorf("PUT missing %q in:\n%s", want, put)
		}
	}

	get := captureRawRequest(t, func(dav *WebDAV) { dav.Get(context.Background(), "x") })
	if strings.Contains(get, "\r\nOrigin:") {
		t.Errorf("GET must not carry Origin (Chrome omits it for same-origin GET):\n%s", get)
	}
	if !strings.Contains(get, "\r\nCache-Control: no-cache\r\n") || !strings.Contains(get, "\r\nPragma: no-cache\r\n") {
		t.Errorf("GET missing no-cache headers:\n%s", get)
	}
}

func TestResponseDecoding(t *testing.T) {
	payload := bytes.Repeat([]byte("webdav-tunnel chunk "), 500)
	encode := map[string]func(io.Writer) io.WriteCloser{
		"gzip":    func(w io.Writer) io.WriteCloser { return gzip.NewWriter(w) },
		"deflate": func(w io.Writer) io.WriteCloser { return zlib.NewWriter(w) },
		"br":      func(w io.Writer) io.WriteCloser { return brotli.NewWriter(w) },
		"zstd": func(w io.Writer) io.WriteCloser {
			zw, _ := zstd.NewWriter(w)
			return zw
		},
		"raw-deflate": func(w io.Writer) io.WriteCloser {
			fw, _ := flate.NewWriter(w, flate.DefaultCompression)
			return fw
		},
	}
	for name, enc := range encode {
		t.Run(name, func(t *testing.T) {
			var body bytes.Buffer
			w := enc(&body)
			w.Write(payload)
			w.Close()
			header := strings.TrimPrefix(name, "raw-")

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", header)
				w.Write(body.Bytes())
			}))
			defer srv.Close()

			got, status, err := NewWebDAV(srv.URL, "u", "p", 5*time.Second, "").Get(context.Background(), "x")
			if err != nil || status != 200 {
				t.Fatalf("Get: status %d, err %v", status, err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("decoded %d bytes, want %d", len(got), len(payload))
			}
		})
	}
}
