# Flags reference

| Flag | Mode | Default | Description |
|------|------|---------|-------------|
| `-mode` | — | required | `client`, `server`, or `selfhosted` |
| `-config` | all | — | Path to a YAML config file ([docs/config.md](config.md)); the way to declare multiple WebDAV backends. CLI flags override its values |
| `-uri` | client | — | Connection URI (`webdav://user:pass@host:port`), replaces `-webdav`/`-login`/`-password` and tuning flags |
| `-webdav` | client, server | required (unless `-config` sets `backends:`) | WebDAV base URL |
| `-login` | all | required (unless `-config` sets `backends:`) | WebDAV username |
| `-password` | all | required (unless `-config` sets `backends:`) | WebDAV password |
| `-enc` | all | `false` | Encrypt chunk data with AES-256-GCM (key derived from WebDAV password) |
| `-timeout` | all | `60s` | HTTP request timeout |
| `-socks-listen` | client | required | Address for the SOCKS5 listener (e.g. `127.0.0.1:1080`) |
| `-socks-user` | client | — | SOCKS5 proxy username |
| `-socks-pass` | client | — | SOCKS5 proxy password |
| `-proxy` | server, selfhosted | — | Upstream SOCKS5 proxy: `socks5://[user:pass@]host:port` |
| `-webdav-listen` | selfhosted | required | Address for the embedded WebDAV server (e.g. `:8080`) |
| `-webdav-storage` | selfhosted | `webdav-data` | Directory for session data |
| `-webdav-tls-cert` | selfhosted | — | TLS certificate file |
| `-webdav-tls-key` | selfhosted | — | TLS key file |
| `-poll-max` | all | `500ms` (selfhosted: `200ms`) | Maximum poll interval when idle |
| `-poll-min` | all | `200ms` (selfhosted: `50ms`) | Starting poll interval (adaptive backoff) |
| `-coalesce` | all | `10ms` (selfhosted: `5ms`) | Write coalescing window |
| `-chunk-size` | all | `131071` | Chunk size in bytes |
| `-puts` | all | `8` | Parallel upload limit |
| `-read-min` | all | `3` | Minimum concurrent prefetch GETs |
| `-read-max` | all | `8` | Maximum concurrent prefetch GETs |
