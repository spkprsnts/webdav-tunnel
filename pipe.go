package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// --- НАСТРОЙКИ ДЛЯ ПУБЛИЧНЫХ ОБЛАКОВ (Яндекс, Mail.ru и др. с лимитами) ---
	pollInterval      = 500 * time.Millisecond // Реже опрашиваем 404, чтобы не злить анти-бот
	chunkDataSize     = 128*1024 - 1           // Оптимальный размер чанка, чтобы не ловить таймауты
	maxConcurrentPuts = 8                      // Лимит параллельных отправок
	readAheadWindow   = 3                      // Уменьшаем ширину окна, чтобы не спамить 404-ошибками

	// --- НАСТРОЙКИ ДЛЯ СВОЕГО VPS (Apache/rclone без лимитов) ---
	// pollInterval      = 100 * time.Millisecond
	// chunkDataSize     = 1024*1024 - 1
	// maxConcurrentPuts = 16
	// readAheadWindow   = 10

	// --- ОБЩИЕ ТАЙМАУТЫ ---
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

	// ctx отменяется при Close() или когда doneCh закрывается.
	ctx    context.Context
	cancel context.CancelFunc

	// doneCh закрывается когда от другой стороны получен сигнал done.
	doneCh   chan struct{}
	doneOnce sync.Once

	writeCh   chan []byte
	readCh    chan []byte
	startOnce sync.Once
	wg        sync.WaitGroup
	putSem    chan struct{}
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
	// Когда doneCh закрывается — немедленно отменяем контекст,
	// чтобы прервать все зависшие HTTP-запросы.
	go func() {
		<-p.doneCh
		p.cancel()
	}()
	return p
}

func (p *Pipe) chunkPath(dir string, seq int64) string {
	return fmt.Sprintf("tunnel/%s/%s/%010d.bin", p.sessionID, dir, seq)
}

// Init создаёт директории и файл init (маркер готовности).
func (p *Pipe) Init() error {
	base := fmt.Sprintf("tunnel/%s", p.sessionID)
	for _, path := range []string{"tunnel", base, base + "/" + p.writeDir, base + "/" + p.readDir} {
		if err := p.dav.Mkcol(p.ctx, path); err != nil {
			return fmt.Errorf("mkcol %s: %w", path, err)
		}
	}
	return p.dav.Put(p.ctx, base+"/init", []byte("ok"))
}

// WriteTarget записывает цель подключения в постоянный файл (не чанк).
func (p *Pipe) WriteTarget(host string, port uint16) error {
	content := fmt.Sprintf("%s\n%d", host, port)
	return p.dav.Put(p.ctx, fmt.Sprintf("tunnel/%s/target", p.sessionID), []byte(content))
}

// ReadTarget ждёт появления target файла. Таймаут: 30 секунд.
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

// StartHeartbeat запускает фоновую горутину, обновляющую hb каждые 30 с.
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

// SignalDone записывает маркер завершения для другой стороны.
// Использует свежий контекст — p.ctx к этому моменту может быть отменён.
func (p *Pipe) SignalDone() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p.dav.Put(ctx, fmt.Sprintf("tunnel/%s/done", p.sessionID), []byte("1"))
}

// WatchDone запускает фоновую горутину, которая опрашивает файл done раз в 3 с.
// Когда файл появляется — закрывает doneCh (что автоматически отменяет p.ctx).
func (p *Pipe) WatchDone() {
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			default:
			}

			_, status, _ := p.dav.Get(p.ctx, fmt.Sprintf("tunnel/%s/done", p.sessionID))
			if status == 200 {
				log.Printf("[%s] получен сигнал done", p.sessionID)
				p.doneOnce.Do(func() { close(p.doneCh) })
				return
			}

			select {
			case <-time.After(doneCheckInterval):
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
	timer := time.NewTimer(25 * time.Millisecond)
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
			eofChunk := isEOF && n == 0 // EOF только когда данных нет — отдельный маркер

			payload := make([]byte, 1+n)
			if eofChunk {
				payload[0] = headerEOF
			} else {
				payload[0] = headerData
			}
			if n > 0 {
				copy(payload[1:], data)
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
					attempts++
					log.Printf("[%s] PUT retry seq %d (attempt %d): %v", p.sessionID, s, attempts, err)
					if attempts >= 15 {
						log.Printf("[%s] fatal: too many PUT errors, aborting pipe", p.sessionID)
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

	// Главный цикл слушает doneCh (не ctx.Done!), чтобы Close() мог
	// сначала закрыть writeCh и дождаться финального flush перед отменой контекста.
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
				timer.Reset(25 * time.Millisecond)
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
	var nextSeq int64 = 1
	type fetchResult struct {
		seq  int64
		data []byte
	}
	fetchDone := make(chan fetchResult, readAheadWindow+2)

	fetchNext := func(seq int64) {
		go func() {
			path := p.chunkPath(p.readDir, seq)
			for {
				select {
				case <-p.ctx.Done():
					return
				default:
				}

				chunk, status, err := p.dav.Get(p.ctx, path)
				if err != nil || status == 404 || len(chunk) == 0 {
					select {
					case <-time.After(pollInterval):
						continue
					case <-p.ctx.Done():
						return
					}
				}
				// Delete в фоне с независимым контекстом: хотим удалить даже если p.ctx отменён.
				go p.dav.Delete(context.Background(), path)
				select {
				case fetchDone <- fetchResult{seq, chunk}:
				case <-p.ctx.Done():
				}
				return
			}
		}()
	}

	for i := 0; i < readAheadWindow; i++ {
		fetchNext(nextSeq + int64(i))
	}

	results := make(map[int64][]byte)
	for {
		select {
		case <-p.ctx.Done():
			return
		case res := <-fetchDone:
			results[res.seq] = res.data
			for {
				if chunk, ok := results[nextSeq]; ok {
					delete(results, nextSeq)

					if chunk[0] == headerEOF {
						close(p.readCh)
						return
					}
					select {
					case p.readCh <- chunk[1:]:
						nextSeq++
						fetchNext(nextSeq + int64(readAheadWindow) - 1)
					case <-p.ctx.Done():
						return
					}
				} else {
					break
				}
			}
		}
	}
}

func (p *Pipe) Write(data []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = io.EOF // паника "send on closed channel" при гонке Close/Write
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
	// Закрываем writeCh → startWriter сделает финальный flush и отправит EOF-чанк.
	p.startOnce.Do(p.start)
	close(p.writeCh)

	// Ждём пока все PUT-горутины завершатся (EOF доставлен другой стороне).
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

	// Только теперь отменяем контекст — прерываем зависшие GET-горутины.
	p.cancel()
}

// Cleanup удаляет директорию сессии на WebDAV.
// Использует свежий контекст — p.ctx к этому моменту уже отменён.
func (p *Pipe) Cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p.dav.Delete(ctx, fmt.Sprintf("tunnel/%s", p.sessionID))
}

// PipeConn адаптирует Pipe к io.ReadWriteCloser для yamux.
// Read буферизует чанки, чтобы удовлетворять произвольные размеры чтения.
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
