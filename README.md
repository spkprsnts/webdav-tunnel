# webdav-tunnel

A TCP tunnel that uses any WebDAV server as a transport layer. Traffic is serialized into files uploaded and downloaded via WebDAV, allowing tunneling through environments where only HTTPS to cloud storage is permitted.

> **Disclaimer:** This project is provided for educational and research purposes only. Use it only on networks and systems you own or have explicit permission to access. The authors are not responsible for any misuse.

## How it works

```
[Browser] → SOCKS5 → [Client] → WebDAV files → [Server] → Internet
```

- The **client** exposes a local SOCKS5 proxy. Each incoming connection opens a [yamux](https://github.com/hashicorp/yamux) stream over a shared WebDAV pipe.
- The **server** polls the same WebDAV storage, picks up sessions, and relays TCP traffic to the destination.
- Data is split into numbered binary chunks stored as files. The reader uses adaptive polling (200 ms → 500 ms backoff) and a read-ahead window to maximize throughput while staying within cloud rate limits.

## Requirements

- Go 1.22+
- Any WebDAV server or cloud storage with WebDAV support

## Build

```sh
go build -o webdav-tunnel .
```

## Usage

### Server (exit node)

```sh
webdav-tunnel \
  -mode server \
  -webdav https://dav.example.com \
  -login user \
  -password <password>
```

### Client (SOCKS5 proxy)

```sh
webdav-tunnel \
  -mode client \
  -webdav https://dav.example.com \
  -login user \
  -password <password> \
  -socks-listen 127.0.0.1:1080
```

Configure your browser or application to use `127.0.0.1:1080` as a SOCKS5 proxy.

## All flags

| Flag | Mode | Description |
|------|------|-------------|
| `-mode` | both | `client` or `server` |
| `-webdav` | both | WebDAV base URL |
| `-login` | both | WebDAV username |
| `-password` | both | WebDAV password |
| `-timeout` | both | HTTP request timeout (default `60s`) |
| `-poll-max` | both | Maximum poll interval when idle (default `500ms`) |
| `-poll-min` | both | Starting poll interval, adaptive backoff (default `200ms`) |
| `-coalesce` | both | Write coalescing window (default `10ms`) |
| `-chunk-size` | both | Chunk size in bytes (default `131071`) |
| `-puts` | both | Parallel upload limit (default `8`) |
| `-read-min` | both | Minimum concurrent prefetch GETs (default `3`) |
| `-read-max` | both | Maximum concurrent prefetch GETs (default `8`) |
| `-socks-listen` | client | Address to listen for SOCKS5 connections (e.g. `127.0.0.1:1080`) |
| `-socks-user` | client | SOCKS5 username for proxy authentication (optional) |
| `-socks-pass` | client | SOCKS5 password for proxy authentication (optional) |
| `-proxy` | server | Upstream SOCKS5 proxy for outbound connections: `socks5://[user:pass@]host:port` |

## Advanced scenarios

### SOCKS5 authentication on the client

Protect the local proxy with a username and password:

```sh
webdav-tunnel -mode client ... \
  -socks-listen 0.0.0.0:1080 \
  -socks-user alice \
  -socks-pass secret
```

### Route server traffic through an upstream SOCKS5 proxy

Useful when the server machine itself sits behind a proxy:

```sh
webdav-tunnel -mode server ... \
  -proxy socks5://proxy.example.com:1080

# with authentication
webdav-tunnel -mode server ... \
  -proxy socks5://user:pass@proxy.example.com:1080
```

## Tuning

The defaults are tuned for public cloud WebDAV. For a self-hosted VPS with no rate limits, use more aggressive values:

```sh
# server
webdav-tunnel -mode server ... \
  -poll-min 50ms -poll-max 100ms \
  -chunk-size 1048575 -puts 16 -read-max 16

# client
webdav-tunnel -mode client ... \
  -poll-min 50ms -poll-max 100ms \
  -chunk-size 1048575 -puts 16 -read-max 16
```

## Android library (gomobile)

The `mobile/` package exposes the client as a gomobile AAR.

### Build

```sh
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init
gomobile bind -target android -o webdav-tunnel.aar webdav-tunnel/mobile
```

### Usage (Java / Kotlin)

```kotlin
import mobile.Mobile

// optional tuning (call before Start)
Mobile.setPollMinMs(50)
Mobile.setChunkSize(1048575)

// start the SOCKS5 proxy on localhost:1080
Mobile.start("https://dav.example.com", "user", "pass", "127.0.0.1:1080", "", "")

// check status / stop
val running = Mobile.isRunning()
Mobile.stop()
```

Configure OkHttp or the system proxy to use `127.0.0.1:1080` as a SOCKS5 proxy.

### DNS support

The SOCKS5 server supports the **UDP ASSOCIATE** command (RFC 1928 §7). When a
client issues UDP ASSOCIATE, the proxy creates a local UDP relay and forwards
DNS queries (port 53) through the WebDAV tunnel as DNS-over-TCP (RFC 1035).
Other UDP traffic is dropped.

This lets SOCKS5-aware DNS clients (e.g. a custom resolver configured to use
the proxy) resolve names through the tunnel without any additional setup.

## Notes

- The tunnel uses **HTTP/1.1 only** (HTTP/2 is disabled). Some cloud providers throttle or fingerprint HTTP/2 bot traffic differently.
- App passwords are recommended over the main account password where supported by the WebDAV provider.
- The server cleans up stale sessions on startup and removes chunk files as they are consumed.
- IPv4 is preferred over IPv6 on the server side to avoid connection hangs on hosts without IPv6 connectivity.
