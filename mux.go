package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
)

const (
	// --- НАСТРОЙКИ YAMUX ---
	// Формула: window = RTT × желаемый_throughput
	muxWindowSize = 24 * 1024 * 1024 // 4 МБ
)

// proxyConfig задаёт upstream SOCKS5-прокси для исходящих соединений сервера.
type proxyConfig struct {
	addr string // host:port прокси
	user string
	pass string
}

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
func proxyMuxLoop(mux *yamux.Session, connCh <-chan net.Conn, user, pass string) {
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
			go proxyStream(mux, conn, preOpened, user, pass)
		case <-mux.CloseChan():
			return
		}
	}
}

// proxyStream обрабатывает одно SOCKS5-соединение через yamux-стрим.
// preOpened — заранее открытый стрим из пула (nil → открываем здесь).
func proxyStream(mux *yamux.Session, conn net.Conn, preOpened net.Conn, user, pass string) {
	defer conn.Close()

	host, port, err := socks5Handshake(conn, user, pass)
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
func serverMuxSession(dav *WebDAV, sid, connectAddr string, proxy *proxyConfig) {
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
		go serverStream(stream, connectAddr, proxy)
	}

	muxSess.Close()
	log.Printf("[%s] mux session closed", sid)
}

// serverStream обрабатывает один yamux-стрим: читает цель и реле-ит трафик.
func serverStream(stream net.Conn, connectAddr string, proxy *proxyConfig) {
	defer stream.Close()

	id := streamCounter.Add(1)

	var host, port string
	if connectAddr != "" {
		var err error
		host, port, err = net.SplitHostPort(connectAddr)
		if err != nil {
			log.Printf("[s%d] invalid connect addr %s: %v", id, connectAddr, err)
			return
		}
	} else {
		h, p, err := readStreamTarget(stream)
		if err != nil {
			return
		}
		host = h
		port = strconv.Itoa(int(p))
	}

	target := net.JoinHostPort(host, port)
	log.Printf("[s%d] connecting to %s", id, target)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	var conn net.Conn
	var err error
	if proxy != nil {
		conn, err = dialViaSocks5(dialCtx, proxy, host, port)
	} else {
		conn, err = dialTarget(dialCtx, host, port)
	}
	if err != nil {
		log.Printf("[s%d] dial %s failed: %v", id, target, err)
		return
	}
	defer conn.Close()

	relayStreams(stream, conn)
	log.Printf("[s%d] closed", id)
}

// dialTarget резолвит hostname и подключается, ставя IPv4 адреса первыми.
// Это предотвращает зависание на серверах без IPv6 — подключение к IPv6-адресу
// может молча висеть до таймаута вместо быстрого "no route to host".
func dialTarget(ctx context.Context, host, port string) (net.Conn, error) {
	if net.ParseIP(host) != nil {
		return (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("lookup %s: %w", host, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses for %s", host)
	}
	// IPv4 раньше IPv6.
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].IP.To4() != nil && addrs[j].IP.To4() == nil
	})
	dialer := &net.Dialer{}
	var lastErr error
	for _, a := range addrs {
		conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(a.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
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
