# Encryption

By default, chunk files on the WebDAV server are stored as plaintext. Traffic is protected by TLS in transit, but the storage provider can read the data at rest. The `-enc` flag enables end-to-end encryption of every chunk.

## Usage

Add `-enc` on the server:

```sh
webdav-tunnel -mode server -enc -webdav https://... -login user -password pass
```

The server includes `enc=1` in the printed client URI — the client picks it up automatically without any extra flags:

```sh
# copy the URI printed by the server and paste it here
webdav-tunnel -mode client -uri "webdav://user:pass@...?enc=1&..." -socks-listen 127.0.0.1:1080
```

Or pass `-enc` explicitly on the client if you're not using `-uri`:

```sh
webdav-tunnel -mode client -enc -webdav https://... -login user -password pass -socks-listen 127.0.0.1:1080
```

## How it works

- The 256-bit AES key is derived from the backend's WebDAV **login and password** with **scrypt** (`N=32768, r=8, p=1`), salted with a hash of the login: `scrypt(password, SHA-256("webdav-tunnel-v2-salt:" + login), N, r, p, 32)`. No separate key material is needed — both sides already know the login and password. scrypt is memory-hard, so guessing a weak password offline costs roughly 50-100ms per attempt instead of nanoseconds for a flat hash.
- The salt is derived from the *login*, not the backend's base URL — this keeps the key identical on both sides even when they see different URLs for the same backend (e.g. a client going through an nginx TLS front-end to a server that binds its embedded WebDAV on localhost). Two backends sharing the same login and password would derive the same key; use distinct logins per backend if that matters.
- In [multi-backend](config.md) setups, each backend's key is derived independently from *that backend's own* login and password — compromising one backend's credentials doesn't expose traffic on the others.
- Each chunk is encrypted with **AES-256-GCM** using a fresh random 12-byte nonce prepended to the ciphertext. GCM provides both confidentiality and integrity — no separate HMAC is needed.
- Overhead: 28 bytes per chunk (12 nonce + 16 GCM tag). Negligible on 128 KB chunks.
- Control files (`init`, `hb`, `done`, `srv-hb`) are not encrypted; they contain only timestamps and short status tokens.

## Notes

- Both sides must have encryption in the same state. If one side has `-enc` and the other does not, the receiver will see decryption failures and keep retrying until the session times out.
- Changing the WebDAV login or password changes the derived key. Update both server and client together.
- **Breaking change:** versions before the scrypt-based KDF derived the key as a flat `SHA-256("webdav-tunnel-v1:" + password)`. An old and a new binary cannot talk to each other with `-enc` enabled — upgrade both sides together.
