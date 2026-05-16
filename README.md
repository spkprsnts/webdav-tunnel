# webdav-tunnel

A TCP tunnel that uses any WebDAV server as a transport layer. Traffic is serialized into files uploaded and downloaded via WebDAV, allowing tunneling through environments where only HTTPS to cloud storage is permitted.

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
| `-socks-listen` | client | Address to listen for SOCKS5 connections (e.g. `127.0.0.1:1080`) |
| `-socks-user` | client | SOCKS5 username for proxy authentication (optional) |
| `-socks-pass` | client | SOCKS5 password for proxy authentication (optional) |
| `-target` | server | Force all streams to a fixed `host:port` instead of using SOCKS5 destination |
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

### Force all connections to a fixed target

Turns the tunnel into a simple TCP forwarder:

```sh
webdav-tunnel -mode server ... -target 192.168.1.10:22
```

## Tuning

Constants in [pipe.go](pipe.go) control throughput vs. rate-limit behaviour.

**Public cloud WebDAV (default)**

| Constant | Value | Notes |
|----------|-------|-------|
| `pollInterval` | 500 ms | Maximum poll backoff when idle |
| `minPollInterval` | 200 ms | Starting poll interval (adaptive) |
| `chunkDataSize` | 128 KB | Chunk size per file |
| `maxConcurrentPuts` | 8 | Parallel uploads |
| `maxReadAheadWindow` | 8 | Parallel prefetch GETs |

**Self-hosted VPS (Apache / rclone, no rate limits)**

Uncomment the VPS block in `pipe.go`:

```go
// pollInterval    = 100 * time.Millisecond
// minPollInterval = 50 * time.Millisecond
// coalesceDelay   = 5 * time.Millisecond
// chunkDataSize   = 1024*1024 - 1
// maxConcurrentPuts = 16
```

## Notes

- The tunnel uses **HTTP/1.1 only** (HTTP/2 is disabled). Some cloud providers throttle or fingerprint HTTP/2 bot traffic differently.
- App passwords are recommended over the main account password where supported by the WebDAV provider.
- The server cleans up stale sessions on startup and removes chunk files as they are consumed.
- IPv4 is preferred over IPv6 on the server side to avoid connection hangs on hosts without IPv6 connectivity.
