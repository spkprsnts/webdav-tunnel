package tunnel

import (
	"bytes"
	"testing"
)

func TestDeriveKeyDeterministic(t *testing.T) {
	k1, err := DeriveKey("user", "pass")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	k2, err := DeriveKey("user", "pass")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	if !bytes.Equal(k1, k2) {
		t.Error("DeriveKey is not deterministic for the same login+password")
	}
	if len(k1) != 32 {
		t.Errorf("len(key) = %d, want 32", len(k1))
	}
}

func TestDeriveKeyDiffersByLogin(t *testing.T) {
	k1, err := DeriveKey("user1", "samepass")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	k2, err := DeriveKey("user2", "samepass")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	if bytes.Equal(k1, k2) {
		t.Error("DeriveKey produced the same key for different logins with the same password")
	}
}

func TestDeriveKeyDiffersByPassword(t *testing.T) {
	k1, err := DeriveKey("sameuser", "pass1")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	k2, err := DeriveKey("sameuser", "pass2")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	if bytes.Equal(k1, k2) {
		t.Error("DeriveKey produced the same key for different passwords with the same login")
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	key, err := DeriveKey("user", "pass")
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	plaintext := []byte("hello, tunnel")

	ciphertext, err := encryptChunk(key, plaintext)
	if err != nil {
		t.Fatalf("encryptChunk: %v", err)
	}
	if bytes.Equal(ciphertext, plaintext) {
		t.Error("ciphertext equals plaintext")
	}

	decrypted, err := decryptChunk(key, ciphertext)
	if err != nil {
		t.Fatalf("decryptChunk: %v", err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestDecryptWithWrongKeyFails(t *testing.T) {
	key1, _ := DeriveKey("user1", "pass1")
	key2, _ := DeriveKey("user2", "pass2")

	ciphertext, err := encryptChunk(key1, []byte("secret"))
	if err != nil {
		t.Fatalf("encryptChunk: %v", err)
	}
	if _, err := decryptChunk(key2, ciphertext); err == nil {
		t.Error("decryptChunk succeeded with the wrong key, want an error")
	}
}
