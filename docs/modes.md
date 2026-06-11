# Modes

## Self-hosted mode

The server runs its own embedded WebDAV — no external storage needed. The simplest way to get started.

**Server:**
```sh
webdav-tunnel \
  -mode selfhosted \
  -webdav-listen :8080 \
  -login myuser \
  -password mypass
```

After startup the server prints a ready-to-use client URI:

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
  -uri "webdav://myuser:mypass@server-ip:8080?..." \
  -socks-listen 127.0.0.1:1080
```

### With TLS

```sh
webdav-tunnel \
  -mode selfhosted \
  -webdav-listen :443 \
  -webdav-tls-cert /etc/ssl/cert.pem \
  -webdav-tls-key  /etc/ssl/key.pem \
  -login myuser \
  -password mypass
```

The printed client URI will use `webdavs://` automatically.

### With SSL via nginx (recommended)

The recommended way to expose the server over HTTPS is to run the embedded WebDAV on localhost and put **nginx** in front for SSL termination. This lets you use a standard certificate (e.g. from Let's Encrypt) without managing TLS in the tunnel process itself.

**1. Start the tunnel on localhost:**
```sh
webdav-tunnel -mode selfhosted -webdav-listen 127.0.0.1:8080 -login myuser -password mypass
```

**2. nginx config:**
```nginx
server {
    listen 443 ssl;
    server_name your.domain.com;

    ssl_certificate     /etc/letsencrypt/live/your.domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your.domain.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;

        # WebDAV requires these to pass through unchanged
        proxy_pass_request_headers on;
        proxy_request_buffering    off;
        proxy_buffering            off;

        # chunk uploads can be large
        client_max_body_size 0;
    }
}
```

**3. Client URI** will use `webdavs://` pointing to the nginx port:
```sh
webdav-tunnel -mode client \
  -uri "webdavs://myuser:mypass@your.domain.com?..." \
  -socks-listen 127.0.0.1:1080
```

The server itself prints the correct `webdavs://` URI if you set `-webdav` to the public HTTPS address before starting — or just replace `webdav://` with `webdavs://` and the IP with your domain in the printed URI.

### With TLS (built-in)

For simple setups without nginx, the embedded server can terminate TLS itself:

```sh
webdav-tunnel \
  -mode selfhosted \
  -webdav-listen :443 \
  -webdav-tls-cert /etc/ssl/cert.pem \
  -webdav-tls-key  /etc/ssl/key.pem \
  -login myuser \
  -password mypass
```

The printed client URI will use `webdavs://` automatically.

### With encryption

Add `-enc` on the server. The URI printed will include `&enc=1` — the client picks it up automatically:

```sh
webdav-tunnel -mode selfhosted -webdav-listen 127.0.0.1:8080 -login myuser -password mypass -enc
```

See [encryption.md](encryption.md) for details.

---

## External WebDAV mode

Use any WebDAV-capable cloud storage (Box, Nextcloud, etc.) as the relay.

**Server (exit node):**
```sh
webdav-tunnel \
  -mode server \
  -webdav https://dav.example.com \
  -login user \
  -password <password>
```

The server prints a client URI. Copy and use it on the client side:

```
server: ════════════════════════════════════════════════════
server: client -uri  webdav://user:<password>@dav.example.com?...
server: ════════════════════════════════════════════════════
```

**Client:**
```sh
webdav-tunnel \
  -mode client \
  -uri "webdav://user:<password>@dav.example.com?..." \
  -socks-listen 127.0.0.1:1080
```

Or with explicit flags instead of `-uri`:
```sh
webdav-tunnel \
  -mode client \
  -webdav https://dav.example.com \
  -login user \
  -password <password> \
  -socks-listen 127.0.0.1:1080
```

Configure your browser or application to use `127.0.0.1:1080` as a SOCKS5 proxy.

---

## Client URI format

The `-uri` flag is a compact alternative to `-webdav`/`-login`/`-password` and tuning flags combined:

```
webdav://user:pass@host:port[?tuning][#name]    →  HTTP
webdavs://user:pass@host:port[?tuning][#name]   →  HTTPS (TLS)
```

The optional `#name` fragment is a human-readable label — never sent to the server. UI clients can use it as a display name for saved configurations:

```
webdav://user:pass@1.2.3.4:8080?...#work-server
webdavs://user:pass@vpn.example.com?...#home
```

All tuning parameters can be embedded in the URI query string. The client applies them for any flag not explicitly set on the command line — an explicit flag always wins:

```sh
# use server's tuning but override poll-max locally
webdav-tunnel -mode client -uri "webdav://..." -socks-listen 127.0.0.1:1080 -poll-max 200ms
```

Supported query parameters: `poll-min`, `poll-max`, `coalesce`, `chunk-size`, `puts`, `read-min`, `read-max`, `enc`.

---

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
