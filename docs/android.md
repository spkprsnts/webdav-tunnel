# Android

## Client app

[WireTurn](https://github.com/spkprsnts/WireTurn) is an Android app with a native UI built on top of the gomobile library. It lets you connect to a webdav-tunnel server, manage saved configurations, and control the SOCKS5 proxy without the command line.

## gomobile library

The `mobile/` package exposes the tunnel client as a gomobile AAR for embedding in Android apps.

### Build

```sh
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init
gomobile bind -target android -o webdav-tunnel.aar webdav-tunnel/mobile
```

### Usage (Kotlin)

```kotlin
import mobile.Mobile

// optional tuning — call before Start
Mobile.setPollMinMs(50)
Mobile.setPollMaxMs(200)
Mobile.setChunkSize(1048575)
Mobile.setConcurrentPuts(16)
Mobile.setReadAheadMin(3)
Mobile.setReadAheadMax(16)

// optional encryption — pass the same WebDAV password used on the server
Mobile.setEncrypt("mypassword")

// start the SOCKS5 proxy on localhost:1080
// socksUser / socksPass: leave empty to disable SOCKS5 auth
Mobile.start("https://dav.example.com", "user", "mypassword", "127.0.0.1:1080", "", "")

// check status
val running = Mobile.isRunning()

// stop
Mobile.stop()
```

Configure OkHttp or the system proxy to use `127.0.0.1:1080` as a SOCKS5 proxy.

### API

| Method | Description |
|--------|-------------|
| `Start(url, login, password, socksListen, socksUser, socksPass)` | Start the tunnel. Verifies WebDAV connectivity before returning. |
| `Stop()` | Stop the tunnel. Safe to call multiple times. |
| `IsRunning()` | Returns `true` if the tunnel is active. |
| `SetEncrypt(password)` | Enable AES-256-GCM encryption. Pass the WebDAV password. |
| `ClearEncrypt()` | Disable encryption (default). |
| `SetPollMinMs(ms)` | Minimum poll interval in milliseconds (default 200). |
| `SetPollMaxMs(ms)` | Maximum poll interval in milliseconds (default 500). |
| `SetCoalesceMs(ms)` | Write coalescing window in milliseconds (default 10). |
| `SetChunkSize(n)` | Chunk size in bytes (default 131071). |
| `SetConcurrentPuts(n)` | Parallel upload limit (default 8). |
| `SetReadAheadMin(n)` | Minimum prefetch GETs (default 3). |
| `SetReadAheadMax(n)` | Maximum prefetch GETs (default 8). |

## DNS support

The SOCKS5 server supports the **UDP ASSOCIATE** command (RFC 1928 §7). DNS queries (port 53) are forwarded through the WebDAV tunnel as DNS-over-TCP (RFC 1035). Other UDP traffic is dropped.

This lets SOCKS5-aware DNS clients resolve names through the tunnel without additional setup.
