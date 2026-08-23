package tunnel

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthStatsSnapshot(t *testing.T) {
	backend := &Backend{Label: "backend-a"}
	backend.Cooldown(time.Minute) // exercise the "unhealthy" branch too

	pool := NewBackendPool([]*Backend{backend, {Label: "backend-b"}})
	h := NewHealthStats()

	h.SessionStarted("backend-b|sess1", "backend-b")
	h.SessionStarted("backend-b|sess2", "backend-b")
	h.SessionEnded("backend-b|sess2")

	resp := h.snapshot(pool)

	if resp.ActiveSessions != 1 {
		t.Errorf("ActiveSessions = %d, want 1", resp.ActiveSessions)
	}
	if resp.TotalSessions != 2 {
		t.Errorf("TotalSessions = %d, want 2 (started counts even after one ended)", resp.TotalSessions)
	}
	if len(resp.Sessions) != 1 || resp.Sessions[0].Key != "backend-b|sess1" {
		t.Errorf("Sessions = %+v, want one entry for backend-b|sess1", resp.Sessions)
	}
	if len(resp.Backends) != 2 {
		t.Fatalf("len(Backends) = %d, want 2", len(resp.Backends))
	}

	var a, b BackendStatus
	for _, bs := range resp.Backends {
		switch bs.Label {
		case "backend-a":
			a = bs
		case "backend-b":
			b = bs
		}
	}
	if a.Healthy {
		t.Error("backend-a should be unhealthy (in cooldown)")
	}
	if a.Retries != 1 {
		t.Errorf("backend-a.Retries = %d, want 1", a.Retries)
	}
	if a.CooldownUntil == nil {
		t.Error("backend-a.CooldownUntil should be set while cooling down")
	}
	if !b.Healthy {
		t.Error("backend-b should be healthy")
	}
}

// TestHealthEndpointServesJSON exercises the actual HTTP handler wiring
// (not just the snapshot struct) by hitting a real httptest server that
// shares the handler registration logic with Serve.
func TestHealthEndpointServesJSON(t *testing.T) {
	pool := NewBackendPool([]*Backend{{Label: "b1"}})
	h := NewHealthStats()
	h.SessionStarted("b1|s1", "b1")

	srv := httptest.NewServer(healthHandler(h, pool))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var parsed healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if parsed.ActiveSessions != 1 {
		t.Errorf("ActiveSessions = %d, want 1", parsed.ActiveSessions)
	}
	if len(parsed.Backends) != 1 || parsed.Backends[0].Label != "b1" {
		t.Errorf("Backends = %+v", parsed.Backends)
	}
}
