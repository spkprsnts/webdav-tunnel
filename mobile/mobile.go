// Package mobile exposes the WebDAV tunnel client as a gomobile library.
//
// Build an Android AAR:
//
//	go install golang.org/x/mobile/cmd/gomobile@latest
//	gomobile init
//	gomobile bind -target android -o webdav-tunnel.aar webdav-tunnel/mobile
package mobile

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"webdav-tunnel/tunnel"
)

var (
	running  atomic.Bool
	cancelFn context.CancelFunc
	encKey   []byte
)

// Start starts the WebDAV SOCKS5 tunnel client.
//
// webdavURL, login, password: WebDAV server credentials.
// socksListen: local address for the SOCKS5 proxy, e.g. "127.0.0.1:1080".
// socksUser, socksPass: optional SOCKS5 authentication (pass empty strings to disable).
//
// The call verifies WebDAV connectivity before returning. Returns an error on
// failure; the tunnel must be stopped with Stop() before calling Start() again.
func Start(webdavURL, login, password, socksListen, socksUser, socksPass string) error {
	if running.Swap(true) {
		return errors.New("tunnel already running")
	}

	dav := tunnel.NewWebDAV(webdavURL, login, password, 60*time.Second)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer pingCancel()
	if err := dav.Ping(pingCtx); err != nil {
		running.Store(false)
		return fmt.Errorf("WebDAV connection failed: %w", err)
	}

	pool := tunnel.NewBackendPool([]*tunnel.Backend{{Label: webdavURL, Dav: dav, EncKey: encKey}})

	ctx, cancel := context.WithCancel(context.Background())
	cancelFn = cancel

	go func() {
		defer running.Store(false)
		if err := tunnel.RunProxy(ctx, pool, socksListen, socksUser, socksPass); err != nil {
			log.Printf("tunnel proxy error: %v", err)
		}
	}()

	return nil
}

// Stop stops the tunnel. Safe to call multiple times.
func Stop() {
	if cancelFn != nil {
		cancelFn()
		cancelFn = nil
	}
	running.Store(false)
}

// IsRunning reports whether the tunnel proxy is active.
func IsRunning() bool {
	return running.Load()
}

// ── tuning ────────────────────────────────────────────────────────────────────
// Call these before Start() to override the defaults.

// SetPollMaxMs sets the maximum poll interval in milliseconds (default 500).
func SetPollMaxMs(ms int) { tunnel.PollInterval = time.Duration(ms) * time.Millisecond }

// SetPollMinMs sets the starting poll interval in milliseconds (default 200).
func SetPollMinMs(ms int) { tunnel.MinPollInterval = time.Duration(ms) * time.Millisecond }

// SetCoalesceMs sets the write coalescing window in milliseconds (default 10).
func SetCoalesceMs(ms int) { tunnel.CoalesceDelay = time.Duration(ms) * time.Millisecond }

// SetChunkSize sets the chunk size in bytes (default 131071).
func SetChunkSize(n int) { tunnel.ChunkDataSize = n }

// SetConcurrentPuts sets the parallel upload limit (default 8).
func SetConcurrentPuts(n int) { tunnel.MaxConcurrentPuts = n }

// SetReadAheadMin sets the minimum concurrent prefetch GETs (default 3).
func SetReadAheadMin(n int) { tunnel.MinReadAheadWindow = n }

// SetReadAheadMax sets the maximum concurrent prefetch GETs (default 8).
func SetReadAheadMax(n int) { tunnel.MaxReadAheadWindow = n }

// SetEncrypt enables AES-256-GCM encryption of tunnel data.
// The key is derived from the WebDAV login and password (via scrypt), so
// both client and server must use the same login+password. Call before
// Start(), with the same login/password values passed to it.
func SetEncrypt(login, password string) error {
	key, err := tunnel.DeriveKey(login, password)
	if err != nil {
		return err
	}
	encKey = key
	return nil
}

// ClearEncrypt disables encryption (default). Call before Start().
func ClearEncrypt() { encKey = nil }
