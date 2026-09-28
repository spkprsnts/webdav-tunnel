package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	PollInterval       = 500 * time.Millisecond // maximum poll backoff while traffic flows
	MinPollInterval    = 200 * time.Millisecond // starting poll interval for adaptive backoff
	PollIdleInterval   = 2 * time.Second        // maximum poll backoff after idleGrace of silence; <= PollInterval disables
	CoalesceDelay      = 10 * time.Millisecond  // write coalescing window
	ChunkDataSize      = 128*1024 - 1           // chunk size chosen to avoid cloud timeouts
	MaxConcurrentPuts  = 8                      // parallel upload limit
	MinReadAheadWindow = 3                      // minimum concurrent GETs (idle baseline)
	MaxReadAheadWindow = 8                      // maximum concurrent GETs under load
)

const (
	idleTimeout = 90 * time.Second

	heartbeatInterval     = 30 * time.Second
	StaleSessionAge       = 90 * time.Second
	doneCheckInterval     = 3 * time.Second
	doneCheckIdleInterval = 15 * time.Second
)

const (
	headerData byte = 0x00
	headerEOF  byte = 0x01
)

// idleGrace is how long a pipe must see no traffic in either direction before
// polling backs off towards PollIdleInterval. Shorter gaps (a page still
// loading, a slow server) keep the PollInterval cap. A var for tests.
var idleGrace = 10 * time.Second

// yamuxHeaderSize is the size of a yamux frame header. yamux writes a frame's
// header and body separately, and keepalive pings and window updates are
// header-only, so writes no larger than this are mux housekeeping: they don't
// hold the pipe out of idle or wake the poller.
const yamuxHeaderSize = 12

// carriesData reports whether a received chunk holds user traffic rather than
// only yamux housekeeping (pings, window updates, empty data frames). Chunks
// are coalesced writes, so an idle one is a run of bare frame headers; anything
// that doesn't parse as that — e.g. a chunk starting mid-body — counts as data.
func carriesData(payload []byte) bool {
	const typeData = 0
	for len(payload) > 0 {
		if len(payload) < yamuxHeaderSize || payload[0] != 0 || payload[1] > 3 {
			return true
		}
		if payload[1] == typeData && binary.BigEndian.Uint32(payload[8:12]) > 0 {
			return true
		}
		payload = payload[yamuxHeaderSize:]
	}
	return false
}

type Pipe struct {
	dav       *WebDAV
	sessionID string
	writeDir  string
	readDir   string
	encKey    []byte // nil = no encryption
	writeSeq  atomic.Int64
	readSeq   atomic.Int64
	closed    atomic.Bool

	// ctx is cancelled on Close() or when doneCh is closed.
	ctx    context.Context
	cancel context.CancelFunc

	// doneCh is closed when the remote side signals done.
	doneCh   chan struct{}
	doneOnce sync.Once

	writeCh   chan []byte
	readCh    chan []byte
	deleteCh  chan string
	startOnce sync.Once
	wg        sync.WaitGroup
	putSem    chan struct{}

	// lastActive is the UnixNano time of the last chunk read or write.
	lastActive atomic.Int64
	// kick wakes the head poller early when this side writes: a reply is
	// likely on its way, so polling drops back to MinPollInterval.
	kick chan struct{}

	// head is the sequence number the reader delivers next. Fetches for later
	// chunks wait for it (headMoved) instead of polling on their own.
	head      atomic.Int64
	headMu    sync.Mutex
	headMoved chan struct{} // closed and replaced when head advances

	// readers tracks startReader and its fetches, which all exit once ctx is
	// cancelled; tests wait on it before changing the polling globals.
	readers sync.WaitGroup

	latMu      sync.Mutex
	latMax     time.Duration
	latSum     time.Duration
	latCount   int
	latLastLog time.Time
}

func NewPipe(dav *WebDAV, sessionID, writeDir, readDir string, encKey []byte) *Pipe {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipe{
		dav:       dav,
		sessionID: sessionID,
		writeDir:  writeDir,
		readDir:   readDir,
		encKey:    encKey,
		ctx:       ctx,
		cancel:    cancel,
		doneCh:    make(chan struct{}),
		writeCh:   make(chan []byte, 128),
		readCh:    make(chan []byte, 128),
		deleteCh:  make(chan string, 1024),
		putSem:    make(chan struct{}, MaxConcurrentPuts),
		kick:      make(chan struct{}, 1),
		headMoved: make(chan struct{}),
	}
	p.head.Store(1)
	p.markActive()
	// When doneCh closes, cancel the context immediately to abort stalled HTTP requests.
	go func() {
		<-p.doneCh
		p.cancel()
	}()
	return p
}

func (p *Pipe) chunkPath(dir string, seq int64) string {
	return fmt.Sprintf("tunnel/%s/%s/%010d.bin", p.sessionID, dir, seq)
}

// Init creates the session directories and the init marker file.
func (p *Pipe) Init() error {
	base := fmt.Sprintf("tunnel/%s", p.sessionID)
	for _, path := range []string{"tunnel", base} {
		if err := p.dav.Mkcol(p.ctx, path); err != nil {
			return fmt.Errorf("mkcol %s: %w", path, err)
		}
	}
	// Subdirectories are independent — create in parallel.
	type mkcolResult struct {
		path string
		err  error
	}
	ch := make(chan mkcolResult, 2)
	for _, sub := range []string{p.writeDir, p.readDir} {
		go func() {
			path := base + "/" + sub
			ch <- mkcolResult{path, p.dav.Mkcol(p.ctx, path)}
		}()
	}
	for range 2 {
		if r := <-ch; r.err != nil {
			return fmt.Errorf("mkcol %s: %w", r.path, r.err)
		}
	}
	return p.dav.Put(p.ctx, base+"/init", []byte("ok"))
}

// WriteTarget writes the connection target to a persistent file (not a chunk).
func (p *Pipe) WriteTarget(host string, port uint16) error {
	content := fmt.Sprintf("%s\n%d", host, port)
	return p.dav.Put(p.ctx, fmt.Sprintf("tunnel/%s/target", p.sessionID), []byte(content))
}

// ReadTarget waits for the target file to appear (timeout: 30 seconds).
func (p *Pipe) ReadTarget() (host string, port uint16, err error) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		data, status, gerr := p.dav.Get(p.ctx, fmt.Sprintf("tunnel/%s/target", p.sessionID))
		if gerr != nil || status == 404 {
			select {
			case <-time.After(PollInterval):
			case <-p.ctx.Done():
				return "", 0, p.ctx.Err()
			}
			continue
		}
		parts := strings.SplitN(strings.TrimSpace(string(data)), "\n", 2)
		if len(parts) != 2 {
			return "", 0, fmt.Errorf("invalid target file")
		}
		p64, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 16)
		if err != nil {
			return "", 0, fmt.Errorf("invalid port: %w", err)
		}
		return strings.TrimSpace(parts[0]), uint16(p64), nil
	}
	return "", 0, fmt.Errorf("timeout waiting for target")
}

// StartHeartbeat starts a background goroutine that updates the hb file every 30 s.
func (p *Pipe) StartHeartbeat(done <-chan struct{}) {
	p.writeHeartbeat()
	go func() {
		t := time.NewTicker(heartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				p.writeHeartbeat()
			}
		}
	}()
}

func (p *Pipe) writeHeartbeat() {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	p.dav.Put(p.ctx, fmt.Sprintf("tunnel/%s/hb", p.sessionID), []byte(ts))
}

// SignalDone writes a done marker for the remote side.
// Uses a fresh context because p.ctx may already be cancelled.
func (p *Pipe) SignalDone() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p.dav.Put(ctx, fmt.Sprintf("tunnel/%s/done", p.sessionID), []byte("1"))
}

// WatchDone starts a background goroutine that polls the done file every 3 s.
// When the file appears it closes doneCh, which in turn cancels p.ctx.
func (p *Pipe) WatchDone() {
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			default:
			}

			_, status, err := p.dav.Get(p.ctx, fmt.Sprintf("tunnel/%s/done", p.sessionID))
			if status == 200 {
				log.Printf("[%s] remote signaled done", p.sessionID)
				p.doneOnce.Do(func() { close(p.doneCh) })
				return
			}

			wait := doneCheckInterval
			if p.idle() {
				wait = doneCheckIdleInterval
			}
			var rlErr *rateLimitError
			if errors.As(err, &rlErr) {
				wait = rlErr.wait
			}
			select {
			case <-time.After(wait):
			case <-p.ctx.Done():
				return
			}
		}
	}()
}

const deleteWorkers = 4

func (p *Pipe) start() {
	go p.startWriter()
	p.readers.Add(1)
	go func() {
		defer p.readers.Done()
		p.startReader()
	}()
	for range deleteWorkers {
		go p.startDeleter()
	}
}

// startDeleter drains deleteCh with a fixed concurrency, preventing the
// unbounded goroutine-per-chunk pattern from starving PUT connections.
func (p *Pipe) startDeleter() {
	for {
		select {
		case path := <-p.deleteCh:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			p.dav.Delete(ctx, path)
			cancel()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pipe) startWriter() {
	var buf []byte
	timer := time.NewTimer(CoalesceDelay)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timerActive := false

	var flush = func(isEOF bool, force bool) {
		if len(buf) == 0 && !isEOF {
			return
		}
		for len(buf) > 0 || isEOF {
			if !isEOF && !force && len(buf) < ChunkDataSize {
				break
			}
			n := len(buf)
			if n > ChunkDataSize {
				n = ChunkDataSize
			}
			var data []byte
			if n > 0 {
				data = buf[:n]
				buf = buf[n:]
			}

			seq := p.writeSeq.Add(1)
			eofChunk := isEOF && n == 0 // EOF-only chunk: separate sentinel when no data remains

			payload := make([]byte, 1+8+n)
			if eofChunk {
				payload[0] = headerEOF
			} else {
				payload[0] = headerData
			}
			binary.BigEndian.PutUint64(payload[1:9], uint64(time.Now().UnixNano()))
			if n > 0 {
				copy(payload[9:], data)
			}

			p.wg.Add(1)
			go func(s int64, b []byte) {
				defer p.wg.Done()
				p.putSem <- struct{}{}
				defer func() { <-p.putSem }()
				if len(p.encKey) > 0 {
					var encErr error
					b, encErr = encryptChunk(p.encKey, b)
					if encErr != nil {
						log.Printf("[%s] encrypt seq=%d: %v", p.sessionID, s, encErr)
						p.doneOnce.Do(func() { close(p.doneCh) })
						return
					}
				}
				path := p.chunkPath(p.writeDir, s)
				writeDir := fmt.Sprintf("tunnel/%s/%s", p.sessionID, p.writeDir)
				attempts := 0
				backoff := 500 * time.Millisecond
				for {
					err := p.dav.Put(p.ctx, path, b)
					if err == nil {
						return
					}
					if p.ctx.Err() != nil {
						return
					}
					var rlErr *rateLimitError
					if errors.As(err, &rlErr) {
						log.Printf("[%s] PUT seq=%d rate limited, waiting %v", p.sessionID, s, rlErr.wait)
						select {
						case <-time.After(rlErr.wait):
						case <-p.ctx.Done():
							return
						}
						continue
					}
					attempts++
					log.Printf("[%s] PUT seq=%d attempt=%d: %v", p.sessionID, s, attempts, err)
					if attempts >= 15 {
						log.Printf("[%s] too many PUT failures, aborting pipe", p.sessionID)
						p.doneOnce.Do(func() { close(p.doneCh) })
						return
					}
					if strings.Contains(err.Error(), "409") {
						p.dav.Mkcol(p.ctx, fmt.Sprintf("tunnel/%s", p.sessionID))
						p.dav.Mkcol(p.ctx, writeDir)
						time.Sleep(500 * time.Millisecond)
					} else {
						select {
						case <-time.After(backoff):
						case <-p.ctx.Done():
							return
						}
						backoff = time.Duration(float64(backoff) * 1.5)
						if backoff > 10*time.Second {
							backoff = 10 * time.Second
						}
					}
				}
			}(seq, payload)

			if eofChunk {
				break
			}
		}
	}

	// Main loop listens on doneCh (not ctx.Done!) so Close() can first close writeCh,
	// wait for the final flush, and only then cancel the context.
	for {
		select {
		case <-p.doneCh:
			return
		case data, ok := <-p.writeCh:
			if !ok {
				flush(true, true)
				return
			}
			buf = append(buf, data...)
			if len(buf) >= ChunkDataSize {
				flush(false, false)
			}
			if len(buf) > 0 && !timerActive {
				timer.Reset(CoalesceDelay)
				timerActive = true
			} else if len(buf) == 0 && timerActive {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timerActive = false
			}
		case <-timer.C:
			timerActive = false
			flush(false, true)
		}
	}
}

func (p *Pipe) startReader() {
	type fetchResult struct {
		seq    int64
		data   []byte
		polled bool
	}
	fetchDone := make(chan fetchResult, MaxReadAheadWindow+4)

	var (
		nextSeq   int64 = 1
		nextFetch int64 = 1
		inFlight  int
		window    = MinReadAheadWindow
	)

	launch := func() {
		seq := nextFetch
		nextFetch++
		inFlight++
		p.readers.Add(1)
		go func() {
			defer p.readers.Done()
			path := p.chunkPath(p.readDir, seq)
			polled := false
			var backoff time.Duration // chosen when this fetch starts polling as head
			for {
				if p.ctx.Err() != nil {
					return
				}
				// Taken before the GET so a head move during it isn't missed.
				headMoved := p.headMovedCh()
				chunk, status, err := p.dav.Get(p.ctx, path)
				var rlErr *rateLimitError
				if errors.As(err, &rlErr) {
					polled = true
					select {
					case <-time.After(rlErr.wait):
						continue
					case <-p.ctx.Done():
						return
					}
				}
				needRetry := err != nil || status == 404
				if !needRetry {
					if len(p.encKey) > 0 {
						var decErr error
						chunk, decErr = decryptChunk(p.encKey, chunk)
						needRetry = decErr != nil || len(chunk) < 9
					} else {
						needRetry = len(chunk) < 9
					}
				}
				if needRetry {
					polled = true
					if seq > p.head.Load() {
						// While the head chunk is missing this one almost
						// certainly is too, and it couldn't be delivered
						// before the head anyway: retry once the head arrives
						// instead of polling on our own.
						select {
						case <-headMoved:
							backoff = 0 // may have waited long; re-pick below
							continue
						case <-p.ctx.Done():
							return
						}
					}
					if backoff == 0 {
						backoff = MinPollInterval
						if p.idle() {
							// Nothing is expected: skip the ramp-up from
							// MinPollInterval that would follow every
							// keepalive. A local write still cuts the wait
							// short (pollWait).
							backoff = p.pollCap()
						}
					}
					wait := backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
					backoff = min(backoff*2, p.pollCap())
					if !p.pollWait(wait, &backoff) {
						return
					}
					continue
				}
				select {
				case p.deleteCh <- path:
				default:
					// queue full — Cleanup() will remove the session dir on close
				}
				select {
				case fetchDone <- fetchResult{seq, chunk, polled}:
				case <-p.ctx.Done():
				}
				return
			}
		}()
	}

	fill := func() {
		for inFlight < window {
			launch()
		}
	}

	fill()

	results := make(map[int64][]byte)
	for {
		select {
		case <-p.ctx.Done():
			return
		case res := <-fetchDone:
			inFlight--
			if !res.polled {
				if window < MaxReadAheadWindow {
					window++
				}
			} else {
				if window > MinReadAheadWindow {
					window--
				}
			}

			results[res.seq] = res.data
			for {
				chunk, ok := results[nextSeq]
				if !ok {
					break
				}
				delete(results, nextSeq)

				ts := int64(binary.BigEndian.Uint64(chunk[1:9]))
				if lat := time.Since(time.Unix(0, ts)); lat > 0 {
					p.recordLatency(lat)
				}

				if chunk[0] == headerEOF {
					close(p.readCh)
					return
				}
				select {
				case p.readCh <- chunk[9:]:
					nextSeq++
					p.advanceHead(nextSeq)
					if carriesData(chunk[9:]) {
						p.markActive()
					}
				case <-p.ctx.Done():
					return
				}
			}
			fill()
		}
	}
}

func (p *Pipe) markActive() { p.lastActive.Store(time.Now().UnixNano()) }

// idle reports whether the pipe has seen no traffic for idleGrace.
func (p *Pipe) idle() bool {
	return time.Since(time.Unix(0, p.lastActive.Load())) > idleGrace
}

// pollCap is the ceiling for the head poller's backoff.
func (p *Pipe) pollCap() time.Duration {
	if PollIdleInterval > PollInterval && p.idle() {
		return PollIdleInterval
	}
	return PollInterval
}

// pollWait sleeps before the head chunk is polled again. A local write cuts a
// long wait down to MinPollInterval and resets the backoff: a reply is likely
// on its way. Returns false if the pipe is closing.
func (p *Pipe) pollWait(wait time.Duration, backoff *time.Duration) bool {
	t := time.NewTimer(wait)
	defer t.Stop()
	deadline := time.Now().Add(wait)
	for {
		select {
		case <-t.C:
			return true
		case <-p.kick:
			*backoff = MinPollInterval
			if time.Until(deadline) > MinPollInterval {
				t.Reset(MinPollInterval)
				deadline = time.Now().Add(MinPollInterval)
			}
		case <-p.ctx.Done():
			return false
		}
	}
}

func (p *Pipe) headMovedCh() <-chan struct{} {
	p.headMu.Lock()
	defer p.headMu.Unlock()
	return p.headMoved
}

// advanceHead publishes the next sequence number to deliver and wakes the
// fetches waiting for it.
func (p *Pipe) advanceHead(seq int64) {
	p.head.Store(seq)
	p.headMu.Lock()
	close(p.headMoved)
	p.headMoved = make(chan struct{})
	p.headMu.Unlock()
}

func (p *Pipe) Write(data []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = io.EOF // "send on closed channel" panic from a Close/Write race
		}
	}()
	p.startOnce.Do(p.start)
	if p.closed.Load() {
		return io.EOF
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	if len(data) > yamuxHeaderSize {
		p.markActive()
		select {
		case p.kick <- struct{}{}:
		default:
		}
	}
	select {
	case p.writeCh <- buf:
		return nil
	case <-p.doneCh:
		return io.EOF
	}
}

func (p *Pipe) Read() ([]byte, error) {
	p.startOnce.Do(p.start)
	timer := time.NewTimer(idleTimeout)
	defer timer.Stop()

	select {
	case <-p.ctx.Done():
		return nil, io.EOF
	case data, ok := <-p.readCh:
		if !ok {
			return nil, io.EOF
		}
		return data, nil
	case <-timer.C:
		return nil, fmt.Errorf("idle timeout after %v", idleTimeout)
	}
}

func (p *Pipe) Close() {
	if p.closed.Swap(true) {
		return
	}
	// Closing writeCh causes startWriter to do a final flush and send the EOF chunk.
	p.startOnce.Do(p.start)
	close(p.writeCh)

	// Wait until all PUT goroutines finish (EOF delivered to the remote side).
	waitCh := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
	case <-p.doneCh:
	case <-time.After(15 * time.Second):
	}

	// Only now cancel the context — aborts stalled GET goroutines.
	p.cancel()
}

// Cleanup deletes the session directory from WebDAV.
// Uses a fresh context because p.ctx is already cancelled by this point.
func (p *Pipe) Cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p.dav.Delete(ctx, fmt.Sprintf("tunnel/%s", p.sessionID))
}

func (p *Pipe) recordLatency(d time.Duration) {
	p.latMu.Lock()
	defer p.latMu.Unlock()
	if d > p.latMax {
		p.latMax = d
	}
	p.latSum += d
	p.latCount++
	now := time.Now()
	if now.Sub(p.latLastLog) >= 10*time.Second {
		if p.latCount >= 5 {
			avg := p.latSum / time.Duration(p.latCount)
			log.Printf("[%s] ← %s latency: avg=%v max=%v chunks=%d",
				p.sessionID, p.readDir,
				avg.Round(time.Millisecond), p.latMax.Round(time.Millisecond), p.latCount)
		}
		p.latMax = 0
		p.latSum = 0
		p.latCount = 0
		p.latLastLog = now
	}
}

// PipeConn adapts Pipe to io.ReadWriteCloser for yamux.
// Read buffers chunks to satisfy arbitrary read sizes.
type PipeConn struct {
	pipe *Pipe
	buf  []byte
	off  int
}

func NewPipeConn(p *Pipe) *PipeConn { return &PipeConn{pipe: p} }

func (pc *PipeConn) Read(p []byte) (int, error) {
	for pc.off >= len(pc.buf) {
		data, err := pc.pipe.Read()
		if err != nil {
			return 0, err
		}
		pc.buf = data
		pc.off = 0
	}
	n := copy(p, pc.buf[pc.off:])
	pc.off += n
	return n, nil
}

func (pc *PipeConn) Write(p []byte) (int, error) {
	if err := pc.pipe.Write(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (pc *PipeConn) Close() error {
	pc.pipe.Close()
	return nil
}
