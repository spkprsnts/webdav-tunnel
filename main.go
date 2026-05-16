package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

func main() {
	mode := flag.String("mode", "", "client | server")
	webdavURL := flag.String("webdav", "", "WebDAV URL (напр. https://webdav.yandex.ru)")
	login := flag.String("login", "", "WebDAV логин")
	password := flag.String("password", "", "WebDAV пароль / пароль приложения")
	listen := flag.String("listen", "", "адрес прослушивания SOCKS5 (для mode client)")
	target := flag.String("target", "", "принудительный target для всех стримов (для mode server, необязательно)")
	timeout := flag.Duration("timeout", 60*time.Second, "таймаут HTTP-запросов")
	flag.Parse()

	if *webdavURL == "" || *login == "" || *password == "" {
		log.Fatal("required flags: -webdav, -login, -password")
	}

	dav := NewWebDAV(*webdavURL, *login, *password, *timeout)

	log.Printf("WebDAV: connecting to %s...", *webdavURL)
	pingStart := time.Now()
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := dav.Ping(pingCtx); err != nil {
		pingCancel()
		log.Fatalf("WebDAV: connection failed: %v", err)
	}
	pingCancel()
	log.Printf("WebDAV: OK (%dms)", time.Since(pingStart).Milliseconds())

	switch *mode {
	case "client":
		if *listen == "" {
			log.Fatal("-listen required for client mode")
		}
		runProxy(dav, *listen)

	case "server":
		runServer(dav, *target)

	default:
		log.Fatal("-mode must be: client | server")
	}
}

// ── proxy (SOCKS5 + yamux) ────────────────────────────────────────────────────
//
// Один WebDAV-пайп на весь клиент. Каждое SOCKS5-соединение — отдельный yamux-стрим.
// При разрыве пайпа клиент автоматически создаёт новый.

func runProxy(dav *WebDAV, listenAddr string) {
	ensureTunnelDir(dav)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", listenAddr, err)
	}
	log.Printf("SOCKS5 proxy listening on %s", listenAddr)

	// Буфер входящих соединений — накапливаются пока mux переподключается
	connCh := make(chan net.Conn, 128)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("accept error: %v", err)
				continue
			}
			connCh <- conn
		}
	}()

	for {
		sid := newSessionID()
		pipe := NewPipe(dav, sid, "c2s", "s2c")
		if err := pipe.Init(); err != nil {
			log.Printf("[%s] pipe init failed: %v", sid, err)
			time.Sleep(3 * time.Second)
			continue
		}

		hbDone := make(chan struct{})
		pipe.StartHeartbeat(hbDone)
		pipe.WatchDone()

		muxSess, err := yamux.Client(NewPipeConn(pipe), yamuxConfig())
		if err != nil {
			log.Printf("[%s] yamux error: %v", sid, err)
			close(hbDone)
			pipe.Cleanup()
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("[%s] mux ready", sid)

		proxyMuxLoop(muxSess, connCh)

		muxSess.Close()
		close(hbDone)
		pipe.Cleanup()
		log.Printf("[%s] mux closed, reconnecting...", sid)
		time.Sleep(time.Second)
	}
}

// ── server (yamux-выход) ──────────────────────────────────────────────────────

func runServer(dav *WebDAV, overrideTarget string) {
	ensureTunnelDir(dav)
	if overrideTarget != "" {
		log.Printf("server mode: all streams forced to %s", overrideTarget)
	} else {
		log.Printf("server mode: dynamic target (SOCKS5 passthrough)")
	}
	go startupCleanup(dav)

	known := make(map[string]struct{})
	var mu sync.Mutex

	for {
		sessions, err := dav.ListSessions(context.Background())
		if err != nil {
			log.Printf("list sessions error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, sid := range sessions {
			mu.Lock()
			_, seen := known[sid]
			if !seen {
				known[sid] = struct{}{}
			}
			mu.Unlock()
			if seen {
				continue
			}
			go func(id string) {
				serverMuxSession(dav, id, overrideTarget)
				// Намеренно не удаляем из known: сессия завершена и не вернётся.
				// Удаление приводило к повторной обработке при медленном удалении на WebDAV.
			}(sid)
		}
		time.Sleep(3 * time.Second)
	}
}

// ── общее ─────────────────────────────────────────────────────────────────────

func startupCleanup(dav *WebDAV) {
	ctx := context.Background()
	hrefs, err := dav.Propfind(ctx, "tunnel")
	if err != nil || len(hrefs) == 0 {
		return
	}
	var sids []string
	for _, href := range hrefs {
		sid := lastPathSegment(href)
		if sid == "" || sid == "tunnel" {
			continue
		}
		sids = append(sids, sid)
	}
	if len(sids) == 0 {
		return
	}
	log.Printf("startup: terminating %d stale sessions", len(sids))
	var wg sync.WaitGroup
	for _, sid := range sids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			NewPipe(dav, id, "s2c", "c2s").SignalDone()
			dav.Delete(ctx, "tunnel/"+id+"/init")
		}(sid)
	}
	wg.Wait()
	time.Sleep(time.Second)
	for _, sid := range sids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			dav.Delete(ctx, "tunnel/"+id)
		}(sid)
	}
	wg.Wait()
	log.Printf("startup: cleanup complete")
}

func ensureTunnelDir(dav *WebDAV) {
	if err := dav.Mkcol(context.Background(), "tunnel"); err != nil {
		log.Printf("warning: mkcol tunnel dir: %v", err)
	}
}

func newSessionID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
