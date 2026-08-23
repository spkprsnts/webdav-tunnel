package tunnel

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"golang.org/x/crypto/scrypt"
)

// scrypt cost parameters — the values recommended by the scrypt package
// docs for interactive use. Deriving a key takes roughly 50-100ms, paid
// once per backend at startup, in exchange for making offline brute-force
// guessing against a weak WebDAV password far more expensive than a flat
// hash would be.
const (
	scryptN = 1 << 15
	scryptR = 8
	scryptP = 1
)

// DeriveKey derives a 32-byte AES-256 key from a backend's WebDAV login and
// password using scrypt. The salt is derived from the login rather than a
// random value: it doesn't need to be exchanged out of band (both sides
// already know the login), and — unlike the backend's base URL — it's
// guaranteed to be identical on the client and server even when they see
// different URLs for the same backend (e.g. a client going through an
// nginx TLS front-end to a server that binds the embedded WebDAV on
// localhost). Using the login as salt means two backends with the same
// login and password still derive the same key; use distinct logins per
// backend if that matters for your threat model.
func DeriveKey(login, password string) ([]byte, error) {
	salt := sha256.Sum256([]byte("webdav-tunnel-v2-salt:" + login))
	return scrypt.Key([]byte(password), salt[:], scryptN, scryptR, scryptP, 32)
}

// encryptChunk encrypts plaintext with AES-256-GCM.
// Output: [12-byte random nonce][ciphertext][16-byte GCM tag].
func encryptChunk(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize()) // 12 bytes
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// decryptChunk decrypts a chunk produced by encryptChunk.
func decryptChunk(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ns := gcm.NonceSize()
	if len(ciphertext) < ns+gcm.Overhead() {
		return nil, fmt.Errorf("ciphertext too short (%d bytes)", len(ciphertext))
	}
	return gcm.Open(nil, ciphertext[:ns], ciphertext[ns:], nil)
}
