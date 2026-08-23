package tunnel

import (
	"log"
	"sync"
	"time"
)

const defaultCooldown = 30 * time.Second

// Backend is one WebDAV storage endpoint in a pool.
type Backend struct {
	Label  string // host, or config index — for logs
	Dav    *WebDAV
	EncKey []byte // nil if encryption disabled

	mu            sync.Mutex
	cooldownUntil time.Time
}

// Cooldown marks the backend as unavailable until d has elapsed.
// d <= 0 uses a default cooldown window.
func (b *Backend) Cooldown(d time.Duration) {
	if d <= 0 {
		d = defaultCooldown
	}
	until := time.Now().Add(d)
	b.mu.Lock()
	b.cooldownUntil = until
	b.mu.Unlock()
	log.Printf("backend %s: cooling down for %v", b.Label, d.Round(time.Millisecond))
}

// inCooldown reports whether the backend is currently cooling down, and until when.
func (b *Backend) inCooldown() (bool, time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Now().Before(b.cooldownUntil), b.cooldownUntil
}

// BackendPool round-robins over a set of backends, skipping ones in cooldown.
type BackendPool struct {
	backends []*Backend
	mu       sync.Mutex
	next     int
}

func NewBackendPool(backends []*Backend) *BackendPool {
	return &BackendPool{backends: backends}
}

// Pick returns the next healthy backend in round-robin order, skipping
// backends currently in cooldown. If every backend is cooling down, it
// returns the one recovering soonest rather than blocking.
func (p *BackendPool) Pick() *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := len(p.backends)
	var best *Backend
	var bestUntil time.Time
	for i := 0; i < n; i++ {
		idx := (p.next + i) % n
		b := p.backends[idx]
		cooling, until := b.inCooldown()
		if !cooling {
			p.next = (idx + 1) % n
			return b
		}
		if best == nil || until.Before(bestUntil) {
			best, bestUntil = b, until
		}
	}
	// All backends are cooling down — use the one that recovers soonest.
	p.next = 0
	return best
}

// All returns every backend in the pool.
func (p *BackendPool) All() []*Backend {
	return p.backends
}
