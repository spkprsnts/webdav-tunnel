# Tuning

Self-hosted mode automatically applies aggressive defaults (fast local disk, no rate limits). For external WebDAV you can tune manually to match your network conditions.

## Parameters

| Parameter | What it controls |
|-----------|-----------------|
| `-poll-min` / `-poll-max` | Adaptive polling backoff range while traffic flows. Lower values reduce latency; higher values reduce the API call rate. |
| `-poll-idle` | Polling ceiling once the tunnel has been quiet for 10 s. See [Idle polling](#idle-polling). |
| `-coalesce` | Window during which small writes are batched into one chunk. Lower = less latency, higher = fewer chunks. |
| `-chunk-size` | Size of each uploaded file in bytes. Larger chunks mean fewer round-trips but higher memory use per stream. |
| `-puts` | Number of chunks uploaded in parallel. Increase on high-bandwidth, low-latency connections. |
| `-read-min` / `-read-max` | Read-ahead window: how many chunks are prefetched concurrently. The tunnel adjusts the window automatically based on hit rate. |

## Example: low-latency server

```sh
# server
webdav-tunnel -mode server ... \
  -poll-min 50ms -poll-max 100ms \
  -chunk-size 1048575 -puts 16 -read-max 16

# client — use matching settings
webdav-tunnel -mode client ... \
  -poll-min 50ms -poll-max 100ms \
  -chunk-size 1048575 -puts 16 -read-max 16
```

The server prints a client URI with its current settings embedded, so you can bake them in once and distribute the URI:

```sh
webdav-tunnel -mode server ... -poll-min 50ms -poll-max 100ms -chunk-size 1048575
# server: client -uri  webdav://...?chunk-size=1048575&poll-max=100ms&poll-min=50ms&...
```

## Idle polling

Each side polls WebDAV for the other side's next chunk, so an open tunnel makes requests even with nothing to carry. To keep that low on rate-limited third-party storage:

- Only the next chunk in sequence is polled. Read-ahead fetches for later chunks wait until it arrives, then retry at once.
- After 10 s without user traffic in either direction, polling backs off up to `-poll-idle` (default `2s`). yamux keepalives and window updates don't count as traffic.
- When a side sends data, its own polling drops straight back to `-poll-min`, since a reply is on its way. So the only added latency is on the first request after a pause: the server notices it within one idle poll (up to `-poll-idle` plus jitter). Replies aren't delayed.
- Selfhosted servers poll their own local storage, so they default to `-poll-idle 0` (off) and add no latency. The client URI they print still carries `poll-idle=2s` for the client side, which is woken by its own requests.

With default settings an idle tunnel makes roughly 110 requests a minute, down from about 700. Set `-poll-idle 0` to trade that back for the lowest latency after pauses.

## Notes

- HTTP/1.1 only — HTTP/2 is disabled. Some cloud providers throttle or fingerprint HTTP/2 bot traffic differently.
- HTTPS backends get a Chrome 133 TLS ClientHello (uTLS) to match the Chrome User-Agent, instead of Go's easily recognizable one. The only deviation from real Chrome is ALPN: `http/1.1` alone rather than `h2, http/1.1`, since the tunnel speaks HTTP/1.1 (JA4 `t13d1516h1_…` vs Chrome's `t13d1516h2_…`; JA3 is unaffected by ALPN). If a server rejects the handshake, fall back with `-tls-fingerprint go`. The loopback connection to an embedded selfhosted backend always uses the standard library.
- Requests carry the headers of a same-origin `fetch()` from Chrome 133 on Windows (`sec-ch-ua*`, `Sec-Fetch-*`, `Origin`/`Referer`, `Accept-Encoding: gzip, deflate, br, zstd`, …), and compressed responses are decoded. Header *order* still follows Go's `net/http` (Host and User-Agent first, the rest sorted) rather than Chrome's; only the WebDAV server sees it, behind TLS.
- App passwords are recommended over the main account password where the WebDAV provider supports them.
- The server cleans up stale sessions on startup and removes chunk files as they are consumed.
- IPv4 is preferred over IPv6 on the server side to avoid connection hangs on hosts without IPv6 connectivity.
