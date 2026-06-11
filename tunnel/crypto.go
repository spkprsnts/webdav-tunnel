package tunnel

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

// DeriveKey derives a 32-byte AES-256 key from the WebDAV password.
// A fixed domain prefix ensures the derived key is independent of any other
// use of the same password string.
func DeriveKey(password string) []byte {
	h := sha256.Sum256(append([]byte("webdav-tunnel-v1:"), password...))
	return h[:]
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
