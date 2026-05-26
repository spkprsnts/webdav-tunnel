# webdav-tunnel

A TCP tunnel that uses any WebDAV server as a transport layer. Traffic is serialized into files uploaded and downloaded via WebDAV, allowing tunneling through environments where only HTTPS to cloud storage is permitted.

> **Disclaimer:** This project is provided for educational and research purposes only. Use it only on networks and systems you own or have explicit permission to access. The authors are not responsible for any misuse.

## How it works

```
[Browser] → SOCKS5 → [Client] → WebDAV files → [Server] → Internet
```

- The **client** exposes a local SOCKS5 proxy. Each incoming connection opens a [yamux](https://github.com/hashicorp/yamux) stream over a shared WebDAV pipe.
- The **server** polls the same WebDAV storage, picks up sessions, and relays TCP traffic to the destination.
- Data is split into numbered binary chunks stored as files. The reader uses adaptive polling and a read-ahead window to maximize throughput while staying within cloud rate limits.

## Requirements

- Go 1.22+
- A WebDAV server (cloud storage or self-hosted — see [Self-hosted mode](#self-hosted-mode))

## Build

```sh
go build -o webdav-tunnel .
```

## Modes

### Self-hosted mode

The server runs its own embedded WebDAV server — no external storage needed.

**Server:**
```sh
webdav-tunnel \
  -mode selfhosted \
  -webdav-listen :8080 \
  -login myuser \
  -password mypass
```

After startup the server prints a ready-to-use client URI with all tuning settings embedded:

```
selfhosted: ════════════════════════════════════════════════════
selfhosted: client -uri  webdav://myuser:mypass@YOUR_SERVER_IP:8080?chunk-size=131071&coalesce=5ms&poll-max=100ms&poll-min=50ms&puts=16&read-max=16&read-min=3
selfhosted: ════════════════════════════════════════════════════
```

Replace `YOUR_SERVER_IP` with the server's public IP or hostname.

**Client** — paste the URI from the server output:
```sh
webdav-tunnel \
  -mode client \
  -uri "webdav://myuser:mypass@server-ip:8080" \
  -socks-listen 127.0.0.1:1080
```

With TLS (`webdavs://`):
```sh
webdav-tunnel \
  -mode selfhosted \
  -webdav-listen :443 \
  -webdav-tls-cert /etc/ssl/cert.pem \
  -webdav-tls-key  /etc/ssl/key.pem \
  -login myuser \
  -password mypass
```

The client URI will use `webdavs://` automatically.

### External WebDAV (server + client)

Use any WebDAV-capable cloud storage (Box, Nextcloud, etc.) as the relay.

**Server (exit node):**
```sh
webdav-tunnel \
  -mode server \
  -webdav https://dav.example.com \
  -login user \
  -password <password>
```

**Client (SOCKS5 proxy):**
```sh
webdav-tunnel \
  -mode client \
  -webdav https://dav.example.com \
  -login user \
  -password <password> \
  -socks-listen 127.0.0.1:1080
```

Configure your browser or application to use `127.0.0.1:1080` as a SOCKS5 proxy.

## Client URI format

The `-uri` flag is a compact alternative to `-webdav`/`-login`/`-password` and tuning flags combined:

```
webdav://user:pass@host:port[?tuning]    →  HTTP
webdavs://user:pass@host:port[?tuning]   →  HTTPS (TLS)
```

In selfhosted mode the server prints the URI with all its current tuning settings baked in as query parameters:

```
webdav://user:pass@1.2.3.4:8080?chunk-size=131071&coalesce=5ms&poll-max=100ms&poll-min=50ms&puts=16&read-max=16&read-min=3
```

The client applies these values for any tuning flag not explicitly set on the command line. An explicit flag always wins:

```sh
# use server's tuning except override poll-max
webdav-tunnel -mode client -uri "webdav://..." -socks-listen 127.0.0.1:1080 -poll-max 200ms
```

## All flags

| Flag | Mode | Default | Description |
|------|------|---------|-------------|
| `-mode` | — | required | `client`, `server`, or `selfhosted` |
| `-uri` | client | — | Connection URI (`webdav://user:pass@host:port`), replaces `-webdav`/`-login`/`-password` |
| `-webdav` | client, server | required | WebDAV base URL |
| `-login` | all | required | WebDAV username |
| `-password` | all | required | WebDAV password |
| `-timeout` | all | `60s` | HTTP request timeout |
| `-poll-max` | all | `500ms` (selfhosted: `100ms`) | Maximum poll interval when idle |
| `-poll-min` | all | `200ms` (selfhosted: `50ms`) | Starting poll interval, adaptive backoff |
| `-coalesce` | all | `10ms` (selfhosted: `5ms`) | Write coalescing window |
| `-chunk-size` | all | `131071` | Chunk size in bytes |
| `-puts` | all | `8` (selfhosted: `16`) | Parallel upload limit |
| `-read-min` | all | `3` | Minimum concurrent prefetch GETs |
| `-read-max` | all | `8` (selfhosted: `16`) | Maximum concurrent prefetch GETs |
| `-socks-listen` | client | required | Address for SOCKS5 listener (e.g. `127.0.0.1:1080`) |
| `-socks-user` | client | — | SOCKS5 username for proxy authentication |
| `-socks-pass` | client | — | SOCKS5 password for proxy authentication |
| `-proxy` | server, selfhosted | — | Upstream SOCKS5 proxy: `socks5://[user:pass@]host:port` |
| `-webdav-listen` | selfhosted | required | Address for embedded WebDAV (e.g. `:8080`) |
| `-webdav-storage` | selfhosted | `webdav-data` | Directory for session data |
| `-webdav-tls-cert` | selfhosted | — | TLS certificate file |
| `-webdav-tls-key` | selfhosted | — | TLS key file |

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

Self-hosted mode automatically applies aggressive defaults (fast local disk, no rate limits). For external WebDAV on a low-latency server you can tune manually:

```sh
# server
webdav-tunnel -mode server ... \
  -poll-min 50ms -poll-max 100ms \
  -chunk-size 1048575 -puts 16 -read-max 16

# client (match to server RTT)
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
