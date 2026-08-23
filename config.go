package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the full YAML config file schema. Any field left unset in the
// YAML file keeps its flag default; an explicit CLI flag always overrides
// a config value (see applyConfig in main.go).
type Config struct {
	Mode        string    `yaml:"mode"`
	SocksListen string    `yaml:"socks-listen"`
	SocksUser   string    `yaml:"socks-user"`
	SocksPass   string    `yaml:"socks-pass"`
	Enc         bool      `yaml:"enc"`
	Timeout     *Duration `yaml:"timeout"`
	Proxy       string    `yaml:"proxy"`
	DNS         string    `yaml:"dns"`

	// Single-backend shorthand — ignored if Backends is non-empty.
	Webdav   string `yaml:"webdav"`
	Login    string `yaml:"login"`
	Password string `yaml:"password"`

	Backends []BackendConfig `yaml:"backends"`
	Tuning   TuningConfig    `yaml:"tuning"`

	// selfhosted mode only
	WebdavListen  string `yaml:"webdav-listen"`
	WebdavStorage string `yaml:"webdav-storage"`
	WebdavTLSCert string `yaml:"webdav-tls-cert"`
	WebdavTLSKey  string `yaml:"webdav-tls-key"`
}

type BackendConfig struct {
	URL      string `yaml:"url"`
	Login    string `yaml:"login"`
	Password string `yaml:"password"`
}

type TuningConfig struct {
	PollMin   *Duration `yaml:"poll-min"`
	PollMax   *Duration `yaml:"poll-max"`
	Coalesce  *Duration `yaml:"coalesce"`
	ChunkSize *int      `yaml:"chunk-size"`
	Puts      *int      `yaml:"puts"`
	ReadMin   *int      `yaml:"read-min"`
	ReadMax   *int      `yaml:"read-max"`
}

// Duration wraps time.Duration so it can be parsed from YAML strings like "50ms".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	return &cfg, nil
}
