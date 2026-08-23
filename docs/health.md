# Health endpoint

`-health-listen 127.0.0.1:9090` (or `health-listen:` in the [YAML config](config.md)) starts a JSON status endpoint at `/health` on all three modes (`client`, `server`, `selfhosted`). Disabled by default.

```sh
webdav-tunnel -mode server -config config.yaml -health-listen 127.0.0.1:9090
curl http://127.0.0.1:9090/health
```

```json
{
  "uptime_seconds": 3612.4,
  "active_sessions": 2,
  "total_sessions": 47,
  "sessions": [
    {"key": "https://dav1.example.com|a1b2c3d4e5f6...", "backend": "https://dav1.example.com", "started_at": "2026-08-23T05:25:22Z", "age_seconds": 812.1}
  ],
  "backends": [
    {"label": "https://dav1.example.com", "healthy": true, "retries": 0},
    {"label": "https://dav2.example.com", "healthy": false, "cooldown_until": "2026-08-23T06:03:10Z", "retries": 3}
  ]
}
```

- `sessions` — currently active tunnel sessions and which backend each landed on. In `client` mode this is at most one entry (a client keeps a single active session at a time). In `server`/`selfhosted` mode it's every session the server is currently relaying.
- `total_sessions` — count of sessions started since the process began, including ones that have since closed.
- `backends` — one entry per [pool](config.md#multi-backend-rotation) backend. `healthy: false` means the backend is in its post-failure cooldown window (`cooldown_until`) and being skipped by rotation until then. `retries` is a running count of how many times that backend has been cooled down (rate limited or unreachable) since startup — a rising number points at a flaky or overloaded backend.

## Notes

- **No authentication.** The endpoint exposes session counts and backend health, not secrets — but still bind it to loopback (`127.0.0.1`) or put it behind a firewall/reverse proxy if you need remote access, the same way you would for any unauthenticated status endpoint.
- A failure to bind the health listener (e.g. port already in use) is logged but does not stop the tunnel — it's best-effort monitoring, not a required dependency.
