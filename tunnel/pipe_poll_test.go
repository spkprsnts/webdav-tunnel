package tunnel

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

// countingDAV is an in-memory WebDAV server that counts GETs per path.
type countingDAV struct {
	mu   sync.Mutex
	gets map[string]int
	h    http.Handler
}

func newCountingDAV(t *testing.T) (*countingDAV, *WebDAV) {
	t.Helper()
	c := &countingDAV{
		gets: make(map[string]int),
		h:    &webdav.Handler{FileSystem: webdav.NewMemFS(), LockSystem: webdav.NewMemLS()},
	}
	srv := httptest.NewServer(c)
	t.Cleanup(srv.Close)
	return c, NewWebDAV(srv.URL, "u", "p", 5*time.Second, "")
}

func (c *countingDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		c.mu.Lock()
		c.gets[r.URL.Path]++
		c.mu.Unlock()
	}
	c.h.ServeHTTP(w, r)
}

// chunkGets returns the GET count per chunk file under dir.
func (c *countingDAV) chunkGets(dir string) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int)
	for p, n := range c.gets {
		if strings.Contains(p, "/"+dir+"/") {
			out[p[strings.LastIndex(p, "/")+1:]] = n
		}
	}
	return out
}

// setPollTuning overrides the polling globals for one test.
func setPollTuning(t *testing.T, min, max, idle, grace time.Duration) {
	t.Helper()
	oMin, oMax, oIdle, oGrace := MinPollInterval, PollInterval, PollIdleInterval, idleGrace
	MinPollInterval, PollInterval, PollIdleInterval, idleGrace = min, max, idle, grace
	t.Cleanup(func() {
		MinPollInterval, PollInterval, PollIdleInterval, idleGrace = oMin, oMax, oIdle, oGrace
	})
}

func newTestPipe(t *testing.T, dav *WebDAV) *Pipe {
	t.Helper()
	p := NewPipe(dav, "sess", "c2s", "s2c", nil)
	if err := p.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(p.cancel)
	return p
}

// pollHead starts the reader and returns how many GETs the head chunk and
// the read-ahead chunks got over d, with nothing ever written.
func pollHead(t *testing.T, d time.Duration) (head, ahead int) {
	t.Helper()
	c, dav := newCountingDAV(t)
	p := newTestPipe(t, dav)
	p.startOnce.Do(p.start)
	time.Sleep(d)
	for name, n := range c.chunkGets("s2c") {
		if name == "0000000001.bin" {
			head = n
		} else {
			ahead += n
		}
	}
	return head, ahead
}

// Only the head chunk is polled; read-ahead fetches wait for it.
func TestReaderPollsOnlyHead(t *testing.T) {
	setPollTuning(t, 20*time.Millisecond, 50*time.Millisecond, 0, time.Hour)
	head, ahead := pollHead(t, time.Second)
	if head < 10 {
		t.Errorf("head chunk polled %d times in 1s, want steady polling", head)
	}
	// Each read-ahead fetch makes one GET, then parks until the head moves.
	if ahead > MinReadAheadWindow-1 {
		t.Errorf("read-ahead chunks polled %d times, want at most %d", ahead, MinReadAheadWindow-1)
	}
}

// Once idle, polling backs off to PollIdleInterval.
func TestReaderIdleBackoff(t *testing.T) {
	setPollTuning(t, 20*time.Millisecond, 50*time.Millisecond, 0, time.Hour)
	active, _ := pollHead(t, 1500*time.Millisecond)

	setPollTuning(t, 20*time.Millisecond, 50*time.Millisecond, 500*time.Millisecond, 0)
	idle, _ := pollHead(t, 1500*time.Millisecond)

	if idle*3 > active {
		t.Errorf("idle pipe polled %d times vs %d while active, want far fewer", idle, active)
	}
}

// A local write wakes an idle head poller, so the reply is picked up quickly
// instead of after a long idle wait.
func TestWriteWakesIdlePoller(t *testing.T) {
	setPollTuning(t, 50*time.Millisecond, 50*time.Millisecond, 5*time.Second, 0)
	_, dav := newCountingDAV(t)
	p := newTestPipe(t, dav)
	p.startOnce.Do(p.start)
	// Waits double from 50ms with up to +50% jitter, so the 1.6s wait starts
	// by 2.33s and ends no earlier than 3.15s: at 2.4s the poller is
	// mid-wait, and without the wake-up the reply can't be seen before 3.15s.
	time.Sleep(2400 * time.Millisecond)

	if err := p.Write([]byte("GET / HTTP/1.1 Host: example.com")); err != nil {
		t.Fatal(err)
	}
	// The remote side's reply lands shortly after.
	time.Sleep(50 * time.Millisecond)
	reply := make([]byte, 9, 9+5)
	reply[0] = headerData
	binary.BigEndian.PutUint64(reply[1:9], uint64(time.Now().UnixNano()))
	reply = append(reply, "reply"...)
	if err := dav.Put(context.Background(), p.chunkPath("s2c", 1), reply); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	got, err := p.Read()
	if err != nil || string(got) != "reply" {
		t.Fatalf("Read = %q, %v", got, err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("reply picked up after %v, want well under the idle interval", d)
	}
}

// Bare yamux headers (keepalive pings, window updates) are housekeeping and
// must not pull an idle pipe out of idle; real payload must.
func TestControlFramesKeepIdle(t *testing.T) {
	setPollTuning(t, 20*time.Millisecond, 50*time.Millisecond, time.Second, 50*time.Millisecond)
	_, dav := newCountingDAV(t)
	p := newTestPipe(t, dav)
	time.Sleep(100 * time.Millisecond)

	p.Write(make([]byte, yamuxHeaderSize)) // ping
	if !p.idle() {
		t.Error("a bare yamux header ended idle")
	}
	p.Write(make([]byte, 100))
	if p.idle() {
		t.Error("payload did not end idle")
	}
}

func TestCarriesData(t *testing.T) {
	hdr := func(typ byte, length uint32) []byte {
		h := make([]byte, yamuxHeaderSize)
		h[1] = typ
		binary.BigEndian.PutUint32(h[8:12], length)
		return h
	}
	ping, winUpdate := hdr(2, 0), hdr(1, 256*1024)
	cases := []struct {
		name    string
		payload []byte
		want    bool
	}{
		{"ping", ping, false},
		{"ping+window update", append(append([]byte{}, ping...), winUpdate...), false},
		{"empty data frame (FIN)", hdr(0, 0), false},
		{"data frame + body", append(hdr(0, 5), "hello"...), true},
		{"mid-body split", []byte("tail of a large frame body"), true},
	}
	for _, c := range cases {
		if got := carriesData(c.payload); got != c.want {
			t.Errorf("%s: carriesData = %v, want %v", c.name, got, c.want)
		}
	}
}
