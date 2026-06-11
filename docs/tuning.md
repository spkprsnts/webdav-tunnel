# Tuning

Self-hosted mode automatically applies aggressive defaults (fast local disk, no rate limits). For external WebDAV you can tune manually to match your network conditions.

## Parameters

| Parameter | What it controls |
|-----------|-----------------|
| `-poll-min` / `-poll-max` | Adaptive polling backoff range. Lower values reduce latency; higher values reduce API call rate when idle. |
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

## Notes

- HTTP/1.1 only — HTTP/2 is disabled. Some cloud providers throttle or fingerprint HTTP/2 bot traffic differently.
- App passwords are recommended over the main account password where the WebDAV provider supports them.
- The server cleans up stale sessions on startup and removes chunk files as they are consumed.
- IPv4 is preferred over IPv6 on the server side to avoid connection hangs on hosts without IPv6 connectivity.
