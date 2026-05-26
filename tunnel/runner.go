package tunnel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
)

// RunProxy starts the SOCKS5 proxy client. Blocks until ctx is cancelled.
// Returns an error if the listener cannot be started.
func RunProxy(ctx context.Context, dav *WebDAV, listenAddr, socksUser, socksPass string) error {
	ensureTunnelDir(dav)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	log.Printf("SOCKS5 proxy listening on %s", listenAddr)

	// Buffer incoming connections while the mux reconnects.
	connCh := make(chan net.Conn, 128)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // closed by ctx or network error
			}
			connCh <- conn
		}
	}()

	for {
		if ctx.Err() != nil {
			return nil
		}
		sid := newSessionID()
		pipe := NewPipe(dav, sid, "c2s", "s2c")
		if err := pipe.Init(); err != nil {
			log.Printf("[%s] pipe init failed: %v", sid, err)
			select {
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
				return nil
			}
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
			select {
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
				return nil
			}
			continue
		}
		log.Printf("[%s] mux ready, waiting for server...", sid)
		go watchServerPresence(ctx, dav, sid, muxSess.CloseChan())

		proxyMuxLoop(ctx, muxSess, connCh, socksUser, socksPass)

		muxSess.Close()
		close(hbDone)
		pipe.Cleanup()

		if ctx.Err() != nil {
			return nil
		}
		log.Printf("[%s] mux closed, reconnecting...", sid)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return nil
		}
	}
}

// RunServer polls for client sessions and handles them. Blocks indefinitely.
func RunServer(dav *WebDAV, proxy *ProxyConfig) {
	ensureTunnelDir(dav)
	log.Printf("server mode: dynamic target (SOCKS5 passthrough)")
	if proxy != nil {
		log.Printf("server mode: routing through SOCKS5 proxy %s", proxy.addr)
	}
	go startupCleanup(dav)

	type sessionEntry struct {
		closedAt time.Time
	}
	known := make(map[string]*sessionEntry)
	var mu sync.Mutex

	// Periodically remove closed sessions (kept for 5 min to avoid re-processing
	// a session while WebDAV file deletion is still in flight).
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			mu.Lock()
			for id, e := range known {
				if !e.closedAt.IsZero() && time.Since(e.closedAt) > 5*time.Minute {
					delete(known, id)
				}
			}
			mu.Unlock()
		}
	}()

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
				known[sid] = &sessionEntry{}
			}
			mu.Unlock()
			if seen {
				continue
			}
			go func(id string) {
				serverMuxSession(dav, id, proxy)
				mu.Lock()
				if e, ok := known[id]; ok {
					e.closedAt = time.Now()
				}
				mu.Unlock()
			}(sid)
		}
		time.Sleep(3 * time.Second)
	}
}

func watchServerPresence(ctx context.Context, dav *WebDAV, sid string, muxClosed <-chan struct{}) {
	hbPath := "tunnel/" + sid + "/srv-hb"

	// Phase 1: wait up to 45 s for the server to write srv-hb.
	deadline := time.NewTimer(45 * time.Second)
	tick := time.NewTicker(3 * time.Second)
waitLoop:
	for {
		select {
		case <-muxClosed:
			deadline.Stop()
			tick.Stop()
			return
		case <-ctx.Done():
			deadline.Stop()
			tick.Stop()
			return
		case <-deadline.C:
			tick.Stop()
			log.Printf("[%s] server has not picked up the session — is the server running?", sid)
			return
		case <-tick.C:
			_, status, _ := dav.Get(ctx, hbPath)
			if status == 200 {
				log.Printf("[%s] server connected", sid)
				deadline.Stop()
				tick.Stop()
				break waitLoop
			}
		}
	}

	// Phase 2: mux closing after a successful connection means server went away.
	select {
	case <-muxClosed:
		if ctx.Err() == nil {
			log.Printf("[%s] server connection lost", sid)
		}
	case <-ctx.Done():
	}
}

func startupCleanup(dav *WebDAV) {
	ctx := context.Background()
	hrefs, err := dav.Propfind(ctx, "tunnel", "1")
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
