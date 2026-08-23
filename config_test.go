package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigFullSchema(t *testing.T) {
	yamlContent := `
mode: client
socks-listen: 127.0.0.1:1080
socks-user: alice
socks-pass: secret
enc: true
timeout: 45s
proxy: socks5://user:pass@proxy.example.com:1080

webdav: https://dav.example.com
login: user
password: pass

backends:
  - url: https://dav1.example.com
    login: user1
    password: pass1
  - url: https://dav2.example.com
    login: user2
    password: pass2

tuning:
  poll-min: 50ms
  poll-max: 200ms
  coalesce: 5ms
  chunk-size: 131071
  puts: 8
  read-min: 3
  read-max: 8

webdav-listen: :8080
webdav-storage: webdav-data
webdav-tls-cert: cert.pem
webdav-tls-key: key.pem
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Mode != "client" {
		t.Errorf("Mode = %q, want client", cfg.Mode)
	}
	if cfg.SocksListen != "127.0.0.1:1080" {
		t.Errorf("SocksListen = %q", cfg.SocksListen)
	}
	if !cfg.Enc {
		t.Error("Enc = false, want true")
	}
	if cfg.Timeout == nil || time.Duration(*cfg.Timeout) != 45*time.Second {
		t.Errorf("Timeout = %v, want 45s", cfg.Timeout)
	}
	if len(cfg.Backends) != 2 {
		t.Fatalf("len(Backends) = %d, want 2", len(cfg.Backends))
	}
	if cfg.Backends[0].URL != "https://dav1.example.com" || cfg.Backends[0].Login != "user1" || cfg.Backends[0].Password != "pass1" {
		t.Errorf("Backends[0] = %+v", cfg.Backends[0])
	}
	if cfg.Backends[1].URL != "https://dav2.example.com" || cfg.Backends[1].Login != "user2" || cfg.Backends[1].Password != "pass2" {
		t.Errorf("Backends[1] = %+v", cfg.Backends[1])
	}
	if cfg.Tuning.PollMin == nil || time.Duration(*cfg.Tuning.PollMin) != 50*time.Millisecond {
		t.Errorf("Tuning.PollMin = %v, want 50ms", cfg.Tuning.PollMin)
	}
	if cfg.Tuning.ChunkSize == nil || *cfg.Tuning.ChunkSize != 131071 {
		t.Errorf("Tuning.ChunkSize = %v, want 131071", cfg.Tuning.ChunkSize)
	}
	if cfg.WebdavListen != ":8080" {
		t.Errorf("WebdavListen = %q", cfg.WebdavListen)
	}
}

func TestLoadConfigOmittedTuningStaysNil(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("mode: server\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Tuning.PollMin != nil {
		t.Errorf("Tuning.PollMin = %v, want nil (absent from YAML)", cfg.Tuning.PollMin)
	}
	if cfg.Timeout != nil {
		t.Errorf("Timeout = %v, want nil (absent from YAML)", cfg.Timeout)
	}
	if len(cfg.Backends) != 0 {
		t.Errorf("Backends = %v, want empty", cfg.Backends)
	}
}

func TestApplyConfigDoesNotOverrideExplicitFlags(t *testing.T) {
	cfg := &Config{
		Mode:        "server",
		SocksListen: "127.0.0.1:9999",
	}
	explicit := map[string]bool{"socks-listen": true}

	mode := ""
	listen := "127.0.0.1:1080" // simulates an explicit CLI flag value
	applyConfig(cfg, explicit, configFlags{
		mode:   &mode,
		listen: &listen,
		// remaining fields left nil-safe by not being touched (all zero values are fine
		// since their config counterparts are empty strings/nil and setStr/setDur/setInt
		// no-op on empty/nil).
		webdavURL: new(string), login: new(string), password: new(string),
		socksUser: new(string), socksPass: new(string), proxyStr: new(string),
		timeout: new(time.Duration), encrypt: new(bool),
		webdavListen: new(string), webdavStorage: new(string),
		webdavTLSCert: new(string), webdavTLSKey: new(string),
		pollMin: new(time.Duration), pollMax: new(time.Duration), coalesce: new(time.Duration),
		chunkSize: new(int), puts: new(int), readAheadMin: new(int), readAheadMax: new(int),
	})

	if mode != "server" {
		t.Errorf("mode = %q, want server (not set on CLI, should come from config)", mode)
	}
	if listen != "127.0.0.1:1080" {
		t.Errorf("listen = %q, want 127.0.0.1:1080 (explicit CLI flag must win over config)", listen)
	}
}
