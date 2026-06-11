# webdav-tunnel

A TCP tunnel that uses any WebDAV server as a transport layer. Traffic is serialized into numbered binary chunk files uploaded and downloaded via WebDAV, allowing tunneling through environments where only HTTPS to cloud storage is permitted.

> **Disclaimer:** This project is provided for educational and research purposes only. Use it only on networks and systems you own or have explicit permission to access. The authors are not responsible for any misuse.

## How it works

```
[Browser] → SOCKS5 → [Client] → WebDAV files → [Server] → Internet
```

The **client** exposes a local SOCKS5 proxy. The **server** polls the same WebDAV storage and relays TCP connections to their destinations. Multiple streams are multiplexed over a single session with [yamux](https://github.com/hashicorp/yamux). Optionally, every chunk is encrypted with **AES-256-GCM** before upload.

## Build

```sh
go build -o webdav-tunnel .
```

Requires Go 1.22+.

## Quick start

The easiest setup is **selfhosted mode** — the server runs its own embedded WebDAV, no external storage needed:

**Server:**
```sh
webdav-tunnel -mode selfhosted -webdav-listen :8080 -login myuser -password mypass
```

The server prints a ready-to-use client URI. Copy it and run on the client machine:

**Client:**
```sh
webdav-tunnel -mode client -uri "webdav://myuser:mypass@SERVER_IP:8080?..." -socks-listen 127.0.0.1:1080
```

Configure your browser or app to use `127.0.0.1:1080` as a SOCKS5 proxy.

## Android

[WireTurn](https://github.com/spkprsnts/WireTurn) is an Android app that wraps this tunnel with a native UI — connect to a server, manage saved configurations, and control the SOCKS5 proxy without the command line.

## Similar projects

- [flowdav](https://github.com/lyafence/flowdav) — similar concept with multi-backend rotation and encrypted config files. **Not compatible** with webdav-tunnel: the two projects use different wire protocols and cannot be mixed (e.g. a webdav-tunnel client cannot connect to a flowdav server and vice versa).

## Documentation

- [Modes](docs/modes.md) — selfhosted, external WebDAV, client URI format, advanced scenarios
- [Encryption](docs/encryption.md) — AES-256-GCM chunk encryption (`-enc` flag)
- [Tuning](docs/tuning.md) — polling, chunk size, concurrency, notes
- [Flags](docs/flags.md) — complete flags reference
- [Android](docs/android.md) — WireTurn app, gomobile library
