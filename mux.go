package main

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
)

const (
	// --- НАСТРОЙКИ YAMUX ---
	// Формула: window = RTT × желаемый_throughput
	// Измеренные RTT на Beeline WebDAV: c2s ~1.9s, s2c ~2.9s
	// При 4 МБ окне: upload ~2 MB/s на стрим, download ~1.4 MB/s на стрим
	muxWindowSize = 24 * 1024 * 1024 // 4 МБ
)

var streamCounter atomic.Int64

func yamuxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = false // используем собственный heartbeat
	cfg.ConnectionWriteTimeout = 120 * time.Second
	cfg.MaxStreamWindowSize = uint32(muxWindowSize)
	cfg.LogOutput = io.Discard
	return cfg
}

// ── proxy: yamux-клиент ───────────────────────────────────────────────────────

const streamPoolSize = 16

// proxyMuxLoop принимает SOCKS5-соединения из connCh и открывает yamux-стримы.
// Поддерживает пул заранее открытых стримов, чтобы устранить задержку SYN/ACK
// (~5s на медленном WebDAV) из критического пути установки соединения.
// Завершается когда mux закрыт.
func proxyMuxLoop(mux *yamux.Session, connCh <-chan net.Conn) {
	pool := make(chan net.Conn, streamPoolSize)

	refill := func() {
		go func() {
			if mux.IsClosed() {
				return
			}
			stream, err := mux.Open()
			if err != nil {
				return
			}
			select {
			case pool <- stream:
			default:
				stream.Close() // пул переполнился пока открывали
			}
		}()
	}

	for i := 0; i < streamPoolSize; i++ {
		refill()
	}

	for {
		if mux.IsClosed() {
			return
		}
		select {
		case conn, ok := <-connCh:
			if !ok {
				return
			}
			if mux.IsClosed() {
				conn.Close()
				return
			}
			var preOpened net.Conn
			select {
			case preOpened = <-pool:
				refill() // сразу восполняем пул
			default:
				// пул пуст — proxyStream откроет сам
			}
			go proxyStream(mux, conn, preOpened)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// proxyStream обрабатывает одно SOCKS5-соединение через yamux-стрим.
// preOpened — заранее открытый стрим из пула (nil → открываем здесь).
func proxyStream(mux *yamux.Session, conn net.Conn, preOpened net.Conn) {
	defer conn.Close()

	host, port, err := socks5Handshake(conn)
	if err != nil {
		if preOpened != nil {
			preOpened.Close()
		}
		return
	}

	var stream net.Conn
	if preOpened != nil {
		stream = preOpened
	} else {
		var openErr error
		stream, openErr = mux.Open()
		if openErr != nil {
			log.Printf("mux stream open failed: %v", openErr)
			conn.Write([]byte{0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
			return
		}
	}
	defer stream.Close()

	// SOCKS5 success: с пулом стрим уже открыт, отвечаем мгновенно.
	// Без пула — только после mux.Open(), чтобы upload-тесты не получали 0.
	if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}

	id := streamCounter.Add(1)
	log.Printf("[s%d] SOCKS5 connect %s:%d", id, host, port)

	if err := writeStreamTarget(stream, host, port); err != nil {
		return
	}

	relayStreams(conn, stream)
	log.Printf("[s%d] closed", id)
}

// ── server: yamux-сервер ──────────────────────────────────────────────────────

// serverMuxSession создаёт yamux-сессию поверх WebDAV-пайпа и принимает стримы.
func serverMuxSession(dav *WebDAV, sid, connectAddr string) {
	if age := dav.SessionAge(context.Background(), sid); age > staleSessionAge {
		log.Printf("[%s] stale session (%v old), removing", sid, age.Round(time.Second))
		// Удаляем init первым — ListSessions перестаёт видеть сессию немедленно,
		// даже если удаление директории зависнет или провалится.
		dav.Delete(context.Background(), "tunnel/"+sid+"/init")
		dav.Delete(context.Background(), "tunnel/"+sid)
		return
	}

	pipe := NewPipe(dav, sid, "s2c", "c2s")
	muxSess, err := yamux.Server(NewPipeConn(pipe), yamuxConfig())
	if err != nil {
		log.Printf("[%s] yamux server error: %v", sid, err)
		return
	}
	log.Printf("[%s] mux accepted", sid)

	for {
		stream, err := muxSess.Accept()
		if err != nil {
			break
		}
		go serverStream(stream, connectAddr)
	}

	muxSess.Close()
	log.Printf("[%s] mux session closed", sid)
}

// serverStream обрабатывает один yamux-стрим: читает цель и реле-ит трафик.
func serverStream(stream net.Conn, connectAddr string) {
	defer stream.Close()

	id := streamCounter.Add(1)

	var target string
	if connectAddr != "" {
		target = connectAddr
	} else {
		host, port, err := readStreamTarget(stream)
		if err != nil {
			return
		}
		target = net.JoinHostPort(host, strconv.Itoa(int(port)))
	}

	log.Printf("[s%d] connecting to %s", id, target)
	conn, err := net.DialTimeout("tcp", target, 15*time.Second)
	if err != nil {
		log.Printf("[s%d] dial %s failed: %v", id, target, err)
		return
	}
	defer conn.Close()

	relayStreams(stream, conn)
	log.Printf("[s%d] closed", id)
}

// ── общее ─────────────────────────────────────────────────────────────────────

func relayStreams(a, b io.ReadWriteCloser) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer b.Close()
		io.Copy(b, a)
	}()
	go func() {
		defer wg.Done()
		defer a.Close()
		io.Copy(a, b)
	}()
	wg.Wait()
}

// writeStreamTarget записывает [2 host_len][host][2 port] в стрим.
func writeStreamTarget(w io.Writer, host string, port uint16) error {
	hb := []byte(host)
	buf := make([]byte, 2+len(hb)+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(hb)))
	copy(buf[2:], hb)
	binary.BigEndian.PutUint16(buf[2+len(hb):], port)
	_, err := w.Write(buf)
	return err
}

// readStreamTarget читает [2 host_len][host][2 port] из стрима.
func readStreamTarget(r io.Reader) (string, uint16, error) {
	var lb [2]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		return "", 0, err
	}
	hb := make([]byte, binary.BigEndian.Uint16(lb[:]))
	if _, err := io.ReadFull(r, hb); err != nil {
		return "", 0, err
	}
	var pb [2]byte
	if _, err := io.ReadFull(r, pb[:]); err != nil {
		return "", 0, err
	}
	return string(hb), binary.BigEndian.Uint16(pb[:]), nil
}
