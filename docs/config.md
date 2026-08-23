# YAML config

`-config path.yaml` loads settings from a YAML file instead of (or alongside)
CLI flags. Its main purpose is declaring **multiple WebDAV backends** — the
one thing that doesn't fit cleanly on the command line — but it can hold any
setting that a flag can.

## Precedence

1. An explicit CLI flag always wins.
2. Otherwise, a value from the config file is used.
3. Otherwise, `-uri` query params are used (client mode).
4. Otherwise, selfhosted auto-defaults apply (selfhosted mode).
5. Otherwise, the built-in flag default is used.

A field left out of the YAML file (or an empty string) is treated as "not
set" — it does not override anything.

## Schema

```yaml
mode: client                 # client | server | selfhosted
socks-listen: 127.0.0.1:1080
socks-user: alice
socks-pass: secret
enc: true
timeout: 60s
proxy: socks5://user:pass@host:port

# Single-backend shorthand — equivalent to -webdav/-login/-password.
# Ignored if `backends` below is non-empty.
webdav: https://dav.example.com
login: user
password: pass

# Multiple backends — the point of this feature. See "Multi-backend
# rotation" below.
backends:
  - url: https://dav1.example.com
    login: user1
    password: pass1
  - url: https://dav2.example.com
    login: user2
    password: pass2

tuning:
  poll-min: 50ms
  poll-max: 200ms
  coalesce: 5ms
  chunk-size: 131071
  puts: 8
  read-min: 3
  read-max: 8

# selfhosted mode only
webdav-listen: :8080
webdav-storage: webdav-data
webdav-tls-cert: ""
webdav-tls-key: ""
```

See [config.example.yaml](../config.example.yaml) for a minimal working
example.

## Multi-backend rotation

When `backends:` lists more than one entry, both the client and the server
must be given the **same list** (same YAML file, or equivalent). Only new
sessions are rotated across backends — a session (one SOCKS5 connection's
tunnel) stays on whichever backend it started on for its whole lifetime:

- **Client**: picks the next healthy backend round-robin for each new
  session. If a backend's `Init()` fails — rate limited (HTTP 429) or
  otherwise unreachable — that backend is put in a cooldown (from the
  server's `Retry-After` header, or 10–30s otherwise) and skipped until it
  recovers.
- **Server**: polls every configured backend for sessions. Whichever backend
  a session is found on is the one the server uses to talk to it. A
  rate-limited backend's poll is skipped (and it's cooled down) without
  blocking discovery on the other backends.
- **Encryption** (`enc: true`): each backend's key is derived from *that
  backend's own* password, not a shared secret. This means backends can use
  different credentials, and a leaked password for one backend does not
  expose traffic on the others.
- **Startup**: all backends are pinged in parallel at startup. Only if
  *every* backend is unreachable does the process exit — one dead backend
  doesn't block startup when others are available.

This is deliberately a session-level fallback, not mid-session failover: an
already-established tunnel keeps using its original backend (relying on the
existing per-request 429 backoff for transient rate limits); only the next
*new* session routes around a backend that's currently cooling down.

Backends don't have to be your own selfhosted instances — each entry is just
a plain WebDAV endpoint with basic auth, so you can mix a selfhosted node
with third-party WebDAV storage (Yandex Disk, Nextcloud, Box, ...) as
fallbacks for each other:

```yaml
backends:
  - url: https://webdav.yandex.ru
    login: you@yandex.ru
    password: app-password
  - url: https://your-selfhosted-server:8080
    login: myuser
    password: mypass
```

## Packing backends into a `-uri` instead of a file

`-config` isn't the only way to share multiple backends — a server started
with `backends:` in its config prints a single `-uri` with all of them
packed in, so the client doesn't need the YAML file at all. See
[modes.md](modes.md#multiple-backends-in-one-uri) for the format.
