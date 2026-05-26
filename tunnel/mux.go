package tunnel

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
	// window = RTT × desired_throughput
	muxWindowSize = 24 * 1024 * 1024

	streamPoolSize = 16
)

// ProxyConfig holds upstream SOCKS5 proxy settings for outbound server connections.
type ProxyConfig struct {
	addr string // host:port
	user string
	pass string
}

// NewProxyConfig creates a ProxyConfig from parsed components.
func NewProxyConfig(addr, user, pass string) *ProxyConfig {
	return &ProxyConfig{addr: addr, user: user, pass: pass}
}

var streamCounter atomic.Int64

func yamuxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = true
	cfg.ConnectionWriteTimeout = 120 * time.Second
	cfg.MaxStreamWindowSize = uint32(muxWindowSize)
	cfg.LogOutput = io.Discard
	return cfg
}

// ── proxy: yamux client ───────────────────────────────────────────────────────

// proxyMuxLoop accepts SOCKS5 connections from connCh and opens yamux streams.
// It maintains a pool of pre-opened streams to eliminate the SYN/ACK latency
// (~5 s on slow WebDAV) from the connection setup hot path.
// Returns when the mux session is closed or ctx is cancelled.
func proxyMuxLoop(ctx context.Context, mux *yamux.Session, connCh <-chan net.Conn, user, pass string) {
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
				stream.Close() // pool full by the time we opened
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
				refill() // replenish immediately
			default:
				// pool empty — proxyStream will open on demand
			}
			go proxyStream(ctx, mux, conn, preOpened, user, pass)
		case <-mux.CloseChan():
			return
		case <-ctx.Done():
			return
		}
	}
}

// proxyStream handles one SOCKS5 connection over a yamux stream.
// preOpened is a stream taken from the pool (nil → open inline).
func proxyStream(ctx context.Context, mux *yamux.Session, conn net.Conn, preOpened net.Conn, user, pass string) {
	defer conn.Close()

	cmd, host, port, err := socks5Handshake(conn, user, pass)
	if err != nil {
		if preOpened != nil {
			preOpened.Close()
		}
		return
	}

	if cmd == 0x03 { // UDP ASSOCIATE — DNS relay over TCP tunnel
		if preOpened != nil {
			preOpened.Close()
		}
		handleUDPAssociate(ctx, conn, mux)
		return
	}

	// CONNECT
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

	// With pool: stream already open, reply immediately.
	// Without pool: reply only after mux.Open() so upload probes don't get a zero.
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

// handleUDPAssociate implements SOCKS5 UDP ASSOCIATE (RFC 1928 §7).
// Only DNS (port 53) datagrams are forwarded — each query is converted to
// DNS-over-TCP (RFC 1035) and sent through the yamux tunnel.
// The TCP control connection (conn) must stay alive while the relay runs;
// closing it (or cancelling ctx) tears down the UDP socket.
func handleUDPAssociate(ctx context.Context, conn net.Conn, mux *yamux.Session) {
	// Bind UDP relay on the same interface as the SOCKS5 listener.
	localIP := conn.LocalAddr().(*net.TCPAddr).IP
	if localIP.IsUnspecified() {
		localIP = net.IPv4(127, 0, 0, 1)
	}
	pc, err := net.ListenPacket("udp", net.JoinHostPort(localIP.String(), "0"))
	if err != nil {
		conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer pc.Close()

	udpPort := pc.LocalAddr().(*net.UDPAddr).Port
	var resp []byte
	if ip4 := localIP.To4(); ip4 != nil {
		resp = []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
		copy(resp[4:8], ip4)
		binary.BigEndian.PutUint16(resp[8:10], uint16(udpPort))
	} else {
		resp = make([]byte, 22)
		resp[0], resp[1], resp[2], resp[3] = 0x05, 0x00, 0x00, 0x04
		copy(resp[4:20], localIP.To16())
		binary.BigEndian.PutUint16(resp[20:22], uint16(udpPort))
	}
	if _, err := conn.Write(resp); err != nil {
		return
	}

	// Close the UDP socket when the TCP control connection closes or ctx is done.
	done := make(chan struct{})
	go func() {
		defer close(done)
		io.Copy(io.Discard, conn)
	}()
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
		}
		pc.Close()
	}()

	buf := make([]byte, 65536)
	for {
		n, src, err := pc.ReadFrom(buf)
		if err != nil {
			return
		}
		pkt := buf[:n]
		// SOCKS5 UDP header: [2 RSV][1 FRAG][1 ATYP][DST.ADDR][2 DST.PORT][DATA]
		if len(pkt) < 4 || pkt[2] != 0 { // drop fragmented or malformed
			continue
		}
		var dstHost string
		var dstPort uint16
		var dataOff int
		switch pkt[3] {
		case 0x01: // IPv4
			if len(pkt) < 10 {
				continue
			}
			dstHost = net.IP(pkt[4:8]).String()
			dstPort = binary.BigEndian.Uint16(pkt[8:10])
			dataOff = 10
		case 0x03: // domain
			if len(pkt) < 5 {
				continue
			}
			dl := int(pkt[4])
			if len(pkt) < 5+dl+2 {
				continue
			}
			dstHost = string(pkt[5 : 5+dl])
			dstPort = binary.BigEndian.Uint16(pkt[5+dl : 5+dl+2])
			dataOff = 5 + dl + 2
		case 0x04: // IPv6
			if len(pkt) < 22 {
				continue
			}
			dstHost = net.IP(pkt[4:20]).String()
			dstPort = binary.BigEndian.Uint16(pkt[20:22])
			dataOff = 22
		default:
			continue
		}
		if dstPort != 53 {
			continue // only DNS is forwarded; other UDP is dropped
		}
		query := make([]byte, n-dataOff)
		copy(query, pkt[dataOff:])
		go relayDNS(pc, src, mux, dstHost, dstPort, query)
	}
}

// relayDNS converts one UDP DNS query to DNS-over-TCP (RFC 1035 §4.2.2),
// tunnels it through a new yamux stream, and returns the UDP response.
func relayDNS(pc net.PacketConn, clientAddr net.Addr, mux *yamux.Session, host string, port uint16, query []byte) {
	stream, err := mux.Open()
	if err != nil {
		return
	}
	defer stream.Close()

	if err := writeStreamTarget(stream, host, port); err != nil {
		return
	}

	tcpQ := make([]byte, 2+len(query))
	binary.BigEndian.PutUint16(tcpQ[:2], uint16(len(query)))
	copy(tcpQ[2:], query)

	stream.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := stream.Write(tcpQ); err != nil {
		return
	}

	var lb [2]byte
	if _, err := io.ReadFull(stream, lb[:]); err != nil {
		return
	}
	ans := make([]byte, binary.BigEndian.Uint16(lb[:]))
	if _, err := io.ReadFull(stream, ans); err != nil {
		return
	}

	// Build SOCKS5 UDP response header with the DNS server as source address.
	var hdr []byte
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			hdr = []byte{0, 0, 0, 0x01, 0, 0, 0, 0, 0, 0}
			copy(hdr[4:8], ip4)
			binary.BigEndian.PutUint16(hdr[8:10], port)
		} else {
			hdr = make([]byte, 22)
			hdr[3] = 0x04
			copy(hdr[4:20], ip.To16())
			binary.BigEndian.PutUint16(hdr[20:22], port)
		}
	} else {
		hdr = make([]byte, 4+1+len(host)+2)
		hdr[3] = 0x03
		hdr[4] = byte(len(host))
		copy(hdr[5:], host)
		binary.BigEndian.PutUint16(hdr[4+1+len(host):], port)
	}

	pc.WriteTo(append(hdr, ans...), clientAddr)
}

// ── server: yamux server ──────────────────────────────────────────────────────

// serverMuxSession creates a yamux session over a WebDAV pipe and accepts streams.
func serverMuxSession(dav *WebDAV, sid string, proxy *ProxyConfig) {
	if age := dav.SessionAge(context.Background(), sid); age > StaleSessionAge {
		log.Printf("[%s] stale session (%v old), removing", sid, age.Round(time.Second))
		// Delete init first so ListSessions stops seeing the session immediately,
		// even if the directory removal stalls or fails.
		dav.Delete(context.Background(), "tunnel/"+sid+"/init")
		dav.Delete(context.Background(), "tunnel/"+sid)
		return
	}

	pipe := NewPipe(dav, sid, "s2c", "c2s")

	// Signal to the client that this session has been picked up.
	dav.Put(context.Background(), "tunnel/"+sid+"/srv-hb",
		[]byte(strconv.FormatInt(time.Now().Unix(), 10)))

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
		go serverStream(stream, proxy)
	}

	muxSess.Close()
	log.Printf("[%s] mux session closed", sid)
}

// serverStream handles one yamux stream: reads the target and relays traffic.
func serverStream(stream net.Conn, proxy *ProxyConfig) {
	defer stream.Close()

	id := streamCounter.Add(1)

	h, p, err := readStreamTarget(stream)
	if err != nil {
		return
	}
	host := h
	port := strconv.Itoa(int(p))

	target := net.JoinHostPort(host, port)
	log.Printf("[s%d] connecting to %s", id, target)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	var conn net.Conn
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

// dialTarget resolves the hostname and dials, putting IPv4 addresses first.
// This prevents hangs on servers without IPv6 — a connection to an IPv6 address
// can silently stall until the timeout instead of failing fast.
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
	// IPv4 before IPv6.
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

// ── shared ────────────────────────────────────────────────────────────────────

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

// writeStreamTarget writes [2 host_len][host][2 port] to the stream.
func writeStreamTarget(w io.Writer, host string, port uint16) error {
	hb := []byte(host)
	buf := make([]byte, 2+len(hb)+2)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(hb)))
	copy(buf[2:], hb)
	binary.BigEndian.PutUint16(buf[2+len(hb):], port)
	_, err := w.Write(buf)
	return err
}

// readStreamTarget reads [2 host_len][host][2 port] from the stream.
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
