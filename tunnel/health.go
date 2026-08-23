package tunnel

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

// HealthStats tracks lightweight runtime state exposed by the optional
// -health-listen endpoint: active sessions and backend pool status. Safe
// for concurrent use.
type HealthStats struct {
	startedAt time.Time

	mu       sync.Mutex
	sessions map[string]sessionInfo
	total    int64
}

type sessionInfo struct {
	Backend   string
	StartedAt time.Time
}

func NewHealthStats() *HealthStats {
	return &HealthStats{startedAt: time.Now(), sessions: make(map[string]sessionInfo)}
}

// SessionStarted records a new active session under key (unique per caller —
// e.g. "backendLabel|sessionID"), attributed to the given backend.
func (h *HealthStats) SessionStarted(key, backend string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sessions[key] = sessionInfo{Backend: backend, StartedAt: time.Now()}
	h.total++
}

// SessionEnded removes key from the active session set. Safe to call even
// if key was never started (no-op).
func (h *HealthStats) SessionEnded(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.sessions, key)
}

type healthResponse struct {
	UptimeSeconds  float64         `json:"uptime_seconds"`
	ActiveSessions int             `json:"active_sessions"`
	TotalSessions  int64           `json:"total_sessions"`
	Sessions       []sessionView   `json:"sessions"`
	Backends       []BackendStatus `json:"backends"`
}

type sessionView struct {
	Key        string    `json:"key"`
	Backend    string    `json:"backend"`
	StartedAt  time.Time `json:"started_at"`
	AgeSeconds float64   `json:"age_seconds"`
}

// snapshot builds the current health response for pool.
func (h *HealthStats) snapshot(pool *BackendPool) healthResponse {
	h.mu.Lock()
	sessions := make([]sessionView, 0, len(h.sessions))
	now := time.Now()
	for key, s := range h.sessions {
		sessions = append(sessions, sessionView{
			Key: key, Backend: s.Backend, StartedAt: s.StartedAt,
			AgeSeconds: now.Sub(s.StartedAt).Seconds(),
		})
	}
	total := h.total
	h.mu.Unlock()

	backends := make([]BackendStatus, 0, len(pool.All()))
	for _, b := range pool.All() {
		backends = append(backends, b.Status())
	}

	return healthResponse{
		UptimeSeconds:  time.Since(h.startedAt).Seconds(),
		ActiveSessions: len(sessions),
		TotalSessions:  total,
		Sessions:       sessions,
		Backends:       backends,
	}
}

// healthHandler builds the /health JSON handler for h and pool.
func healthHandler(h *HealthStats, pool *BackendPool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(h.snapshot(pool))
	})
	return mux
}

// Serve starts the health HTTP endpoint at listenAddr, blocking until it
// fails (e.g. address already in use). Runs in its own goroutine; health
// monitoring is best-effort, so a failure here only logs — it never takes
// down the tunnel itself. Intended for loopback/firewalled binding: the
// endpoint has no authentication.
func (h *HealthStats) Serve(listenAddr string, pool *BackendPool) {
	log.Printf("health endpoint listening on http://%s/health", listenAddr)
	if err := http.ListenAndServe(listenAddr, healthHandler(h, pool)); err != nil {
		log.Printf("health endpoint stopped: %v", err)
	}
}
