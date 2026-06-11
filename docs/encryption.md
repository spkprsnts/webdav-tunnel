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

- The 256-bit AES key is derived from the WebDAV password: `SHA-256("webdav-tunnel-v1:" + password)`. No separate key material is needed — both sides share the same WebDAV password and derive the same key.
- Each chunk is encrypted with **AES-256-GCM** using a fresh random 12-byte nonce prepended to the ciphertext. GCM provides both confidentiality and integrity — no separate HMAC is needed.
- Overhead: 28 bytes per chunk (12 nonce + 16 GCM tag). Negligible on 128 KB chunks.
- Control files (`init`, `hb`, `done`, `srv-hb`) are not encrypted; they contain only timestamps and short status tokens.

## Notes

- Both sides must have encryption in the same state. If one side has `-enc` and the other does not, the receiver will see decryption failures and keep retrying until the session times out.
- Changing the WebDAV password changes the derived key. Update both server and client together.
