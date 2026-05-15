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
		log.Fatal("обязательны флаги: -webdav, -login, -password")
	}

	dav := NewWebDAV(*webdavURL, *login, *password, *timeout)

	switch *mode {
	case "client":
		if *listen == "" {
			log.Fatal("-listen обязателен для mode client")
		}
		runProxy(dav, *listen)

	case "server":
		runServer(dav, *target)

	default:
		log.Fatal("-mode должен быть: client | server")
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
	log.Printf("SOCKS5 proxy слушает на %s", listenAddr)

	// Буфер входящих соединений — накапливаются пока mux переподключается
	connCh := make(chan net.Conn, 128)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Printf("accept: %v", err)
				continue
			}
			connCh <- conn
		}
	}()

	for {
		sid := newSessionID()
		pipe := NewPipe(dav, sid, "c2s", "s2c")
		if err := pipe.Init(); err != nil {
			log.Printf("[%s] pipe init: %v", sid, err)
			time.Sleep(3 * time.Second)
			continue
		}

		hbDone := make(chan struct{})
		pipe.StartHeartbeat(hbDone)
		pipe.WatchDone()

		muxSess, err := yamux.Client(NewPipeConn(pipe), yamuxConfig())
		if err != nil {
			log.Printf("[%s] yamux client: %v", sid, err)
			close(hbDone)
			pipe.Cleanup()
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("[%s] mux установлен", sid)

		proxyMuxLoop(muxSess, connCh)

		muxSess.Close()
		close(hbDone)
		pipe.Cleanup()
		log.Printf("[%s] mux закрыт, переподключение...", sid)
		time.Sleep(time.Second)
	}
}

// ── server (yamux-выход) ──────────────────────────────────────────────────────

func runServer(dav *WebDAV, overrideTarget string) {
	ensureTunnelDir(dav)
	if overrideTarget != "" {
		log.Printf("server: SOCKS5-выход, все стримы → %s", overrideTarget)
	} else {
		log.Printf("server: SOCKS5-выход (динамический адрес)")
	}
	go startupCleanup(dav)

	known := make(map[string]bool)
	var mu sync.Mutex

	for {
		sessions, err := dav.ListSessions(context.Background())
		if err != nil {
			log.Printf("list sessions: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, sid := range sessions {
			mu.Lock()
			if known[sid] {
				mu.Unlock()
				continue
			}
			known[sid] = true
			mu.Unlock()

			go func(id string) {
				serverMuxSession(dav, id, overrideTarget)
				mu.Lock()
				delete(known, id)
				mu.Unlock()
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
	log.Printf("startup: завершаю %d сессий в фоне", len(sids))
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
	log.Printf("startup: cleanup завершён")
}

func ensureTunnelDir(dav *WebDAV) {
	if err := dav.Mkcol(context.Background(), "tunnel"); err != nil {
		log.Printf("warning: mkcol tunnel: %v", err)
	}
}

func newSessionID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
