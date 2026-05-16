package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	pollInterval       = 500 * time.Millisecond // maximum poll backoff when idle
	minPollInterval    = 200 * time.Millisecond // starting poll interval for adaptive backoff
	coalesceDelay      = 10 * time.Millisecond  // write coalescing window
	chunkDataSize      = 128*1024 - 1           // chunk size chosen to avoid cloud timeouts
	maxConcurrentPuts  = 8                      // parallel upload limit
	minReadAheadWindow = 3                      // minimum concurrent GETs (idle baseline)
	maxReadAheadWindow = 8                      // maximum concurrent GETs under load
)

const (
	idleTimeout = 90 * time.Second

	heartbeatInterval = 30 * time.Second
	staleSessionAge   = 90 * time.Second
	doneCheckInterval = 3 * time.Second
)

const (
	headerData byte = 0x00
	headerEOF  byte = 0x01
)

type Pipe struct {
	dav       *WebDAV
	sessionID string
	writeDir  string
	readDir   string
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
	startOnce sync.Once
	wg        sync.WaitGroup
	putSem    chan struct{}

	latMu      sync.Mutex
	latMax     time.Duration
	latSum     time.Duration
	latCount   int
	latLastLog time.Time
}

func NewPipe(dav *WebDAV, sessionID, writeDir, readDir string) *Pipe {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipe{
		dav:       dav,
		sessionID: sessionID,
		writeDir:  writeDir,
		readDir:   readDir,
		ctx:       ctx,
		cancel:    cancel,
		doneCh:    make(chan struct{}),
		writeCh:   make(chan []byte, 128),
		readCh:    make(chan []byte, 128),
		putSem:    make(chan struct{}, maxConcurrentPuts),
	}
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
			case <-time.After(pollInterval):
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

func (p *Pipe) start() {
	go p.startWriter()
	go p.startReader()
}

func (p *Pipe) startWriter() {
	var buf []byte
	timer := time.NewTimer(coalesceDelay)
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
			if !isEOF && !force && len(buf) < chunkDataSize {
				break
			}
			n := len(buf)
			if n > chunkDataSize {
				n = chunkDataSize
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
			if len(buf) >= chunkDataSize {
				flush(false, false)
			}
			if len(buf) > 0 && !timerActive {
				timer.Reset(coalesceDelay)
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
	fetchDone := make(chan fetchResult, maxReadAheadWindow+4)

	var (
		nextSeq   int64 = 1
		nextFetch int64 = 1
		inFlight  int
		window    = minReadAheadWindow
	)

	launch := func() {
		seq := nextFetch
		nextFetch++
		inFlight++
		go func() {
			path := p.chunkPath(p.readDir, seq)
			polled := false
			backoff := minPollInterval
			for {
				if p.ctx.Err() != nil {
					return
				}
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
				if err != nil || status == 404 || len(chunk) == 0 {
					polled = true
					wait := backoff
					backoff *= 2
					if backoff > pollInterval {
						backoff = pollInterval
					}
					select {
					case <-time.After(wait):
						continue
					case <-p.ctx.Done():
						return
					}
				}
				go func(chunkPath string) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					p.dav.Delete(ctx, chunkPath)
				}(path)
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
				if window < maxReadAheadWindow {
					window++
				}
			} else {
				if window > minReadAheadWindow {
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

				if len(chunk) < 9 {
					log.Printf("[%s] malformed chunk seq=%d len=%d", p.sessionID, nextSeq, len(chunk))
					p.doneOnce.Do(func() { close(p.doneCh) })
					return
				}
				ts := int64(binary.BigEndian.Uint64(chunk[1:9]))
				p.recordLatency(time.Since(time.Unix(0, ts)))

				if chunk[0] == headerEOF {
					close(p.readCh)
					return
				}
				select {
				case p.readCh <- chunk[9:]:
					nextSeq++
				case <-p.ctx.Done():
					return
				}
			}
			fill()
		}
	}
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
		avg := p.latSum / time.Duration(p.latCount)
		log.Printf("[%s] ← %s latency: avg=%v max=%v chunks=%d",
			p.sessionID, p.readDir,
			avg.Round(time.Millisecond), p.latMax.Round(time.Millisecond), p.latCount)
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
