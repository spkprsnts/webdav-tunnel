package main

import (
	"context"
	"flag"
	"log"
	"net/url"
	"time"

	"webdav-tunnel/tunnel"
)

func main() {
	mode     := flag.String("mode", "", "client | server")
	webdavURL := flag.String("webdav", "", "WebDAV base URL")
	login    := flag.String("login", "", "WebDAV username")
	password := flag.String("password", "", "WebDAV password / app password")
	listen   := flag.String("socks-listen", "", "address to listen for SOCKS5 connections (client mode)")
	socksUser := flag.String("socks-user", "", "SOCKS5 proxy username (client mode, optional)")
	socksPass := flag.String("socks-pass", "", "SOCKS5 proxy password (client mode, optional)")
	proxyStr := flag.String("proxy", "", "upstream SOCKS5 proxy for the server: socks5://[user:pass@]host:port")
	timeout  := flag.Duration("timeout", 60*time.Second, "HTTP request timeout")

	pollMax      := flag.Duration("poll-max", tunnel.PollInterval, "maximum poll interval when idle")
	pollMin      := flag.Duration("poll-min", tunnel.MinPollInterval, "starting poll interval (adaptive backoff)")
	coalesce     := flag.Duration("coalesce", tunnel.CoalesceDelay, "write coalescing window")
	chunkSize    := flag.Int("chunk-size", tunnel.ChunkDataSize, "chunk size in bytes")
	puts         := flag.Int("puts", tunnel.MaxConcurrentPuts, "parallel upload limit")
	readAheadMin := flag.Int("read-min", tunnel.MinReadAheadWindow, "minimum concurrent prefetch GETs")
	readAheadMax := flag.Int("read-max", tunnel.MaxReadAheadWindow, "maximum concurrent prefetch GETs")
	flag.Parse()

	tunnel.PollInterval       = *pollMax
	tunnel.MinPollInterval    = *pollMin
	tunnel.CoalesceDelay      = *coalesce
	tunnel.ChunkDataSize      = *chunkSize
	tunnel.MaxConcurrentPuts  = *puts
	tunnel.MinReadAheadWindow = *readAheadMin
	tunnel.MaxReadAheadWindow = *readAheadMax

	if *webdavURL == "" || *login == "" || *password == "" {
		log.Fatal("required flags: -webdav, -login, -password")
	}

	dav := tunnel.NewWebDAV(*webdavURL, *login, *password, *timeout)

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
			log.Fatal("-socks-listen required for client mode")
		}
		if *socksUser != "" {
			log.Printf("SOCKS5 auth enabled for user %q", *socksUser)
		}
		if err := tunnel.RunProxy(context.Background(), dav, *listen, *socksUser, *socksPass); err != nil {
			log.Fatalf("proxy: %v", err)
		}

	case "server":
		var proxy *tunnel.ProxyConfig
		if *proxyStr != "" {
			u, err := url.Parse(*proxyStr)
			if err != nil || u.Scheme != "socks5" || u.Host == "" {
				log.Fatal("-proxy must be socks5://[user:pass@]host:port")
			}
			user, pass := "", ""
			if u.User != nil {
				user = u.User.Username()
				pass, _ = u.User.Password()
			}
			proxy = tunnel.NewProxyConfig(u.Host, user, pass)
		}
		tunnel.RunServer(dav, proxy)

	default:
		log.Fatal("-mode must be: client | server")
	}
}
