package tunnel

import (
	"testing"
	"time"
)

func newTestBackend(label string) *Backend {
	return &Backend{Label: label}
}

func TestBackendPoolRoundRobin(t *testing.T) {
	a, b, c := newTestBackend("a"), newTestBackend("b"), newTestBackend("c")
	pool := NewBackendPool([]*Backend{a, b, c})

	got := []string{
		pool.Pick().Label,
		pool.Pick().Label,
		pool.Pick().Label,
		pool.Pick().Label,
	}
	want := []string{"a", "b", "c", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pick %d = %q, want %q (full sequence: %v)", i, got[i], want[i], got)
		}
	}
}

func TestBackendPoolSkipsCooldown(t *testing.T) {
	a, b, c := newTestBackend("a"), newTestBackend("b"), newTestBackend("c")
	pool := NewBackendPool([]*Backend{a, b, c})

	b.Cooldown(time.Minute)

	got := []string{
		pool.Pick().Label,
		pool.Pick().Label,
		pool.Pick().Label,
	}
	want := []string{"a", "c", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pick %d = %q, want %q (full sequence: %v)", i, got[i], want[i], got)
		}
	}
}

func TestBackendPoolRecoversAfterCooldownExpires(t *testing.T) {
	a, b := newTestBackend("a"), newTestBackend("b")
	pool := NewBackendPool([]*Backend{a, b})

	b.Cooldown(10 * time.Millisecond)
	if got := pool.Pick().Label; got != "a" {
		t.Fatalf("pick 1 = %q, want a", got)
	}
	time.Sleep(20 * time.Millisecond)
	if got := pool.Pick().Label; got != "b" {
		t.Fatalf("pick 2 = %q, want b (cooldown should have expired)", got)
	}
}

func TestBackendPoolAllCoolingDownReturnsSoonestRecovery(t *testing.T) {
	a, b := newTestBackend("a"), newTestBackend("b")
	pool := NewBackendPool([]*Backend{a, b})

	a.Cooldown(time.Minute)
	b.Cooldown(time.Second)

	if got := pool.Pick().Label; got != "b" {
		t.Fatalf("pick = %q, want b (recovers soonest)", got)
	}
}
