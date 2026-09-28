package tunnel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"sync"
	"time"
)

// UDP relay over yamux.
//
// A SOCKS5 UDP association opens (lazily, on its first non-DNS datagram) one
// yamux stream whose target header carries udpStreamMarker instead of a real
// host. Both directions then carry length-prefixed frames:
//
//	[2 frame_len][ATYP][ADDR][2 PORT][DATA]
//
// i.e. a SOCKS5 UDP datagram without its RSV/FRAG prefix. Client→server the
// address is the destination, server→client it is the source. The server keeps
// one UDP socket per association and forwards replies from any peer back.
//
// Old servers don't know the marker: they fail to resolve it as a hostname and
// close the stream, and the client falls back to DNS-only.

const (
	udpStreamMarker = "\x00udp"
	maxUDPFrame     = 65535

	udpResolveTTL     = 60 * time.Second
	udpResolveTimeout = 5 * time.Second
	udpResolveMax     = 256 // cache entries per association
)

// writeUDPFrame writes one length-prefixed frame. Oversized frames are dropped,
// as an oversized datagram would be.
func writeUDPFrame(w io.Writer, frame []byte) error {
	if len(frame) > maxUDPFrame {
		return nil
	}
	buf := make([]byte, 2+len(frame))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(frame)))
	copy(buf[2:], frame)
	_, err := w.Write(buf)
	return err
}

// readUDPFrame reads one frame into buf (at least maxUDPFrame bytes) and
// returns its length.
func readUDPFrame(r io.Reader, buf []byte) (int, error) {
	var lb [2]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		return 0, err
	}
	n := int(binary.BigEndian.Uint16(lb[:]))
	if _, err := io.ReadFull(r, buf[:n]); err != nil {
		return 0, err
	}
	return n, nil
}

// parseSocksAddr parses [ATYP][ADDR][2 PORT] and returns the address and the
// number of bytes consumed.
func parseSocksAddr(b []byte) (host string, port uint16, n int, ok bool) {
	if len(b) < 1 {
		return
	}
	switch b[0] {
	case 0x01: // IPv4
		if len(b) < 7 {
			return
		}
		host, n = net.IP(b[1:5]).String(), 5
	case 0x03: // domain
		if len(b) < 2 {
			return
		}
		dl := int(b[1])
		if len(b) < 2+dl+2 {
			return
		}
		host, n = string(b[2:2+dl]), 2+dl
	case 0x04: // IPv6
		if len(b) < 19 {
			return
		}
		host, n = net.IP(b[1:17]).String(), 17
	default:
		return
	}
	return host, binary.BigEndian.Uint16(b[n : n+2]), n + 2, true
}

// appendSocksAddr appends [ATYP][ADDR][2 PORT] for host:port.
func appendSocksAddr(dst []byte, host string, port uint16) []byte {
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			dst = append(dst, 0x01)
			dst = append(dst, ip4...)
		} else {
			dst = append(dst, 0x04)
			dst = append(dst, ip.To16()...)
		}
	} else {
		dst = append(dst, 0x03, byte(len(host)))
		dst = append(dst, host...)
	}
	return binary.BigEndian.AppendUint16(dst, port)
}

// ── server ────────────────────────────────────────────────────────────────────

// udpOutbound is where the server sends an association's datagrams: straight
// to the internet, or through the upstream SOCKS5 proxy. Frames are
// [ATYP][ADDR][2 PORT][DATA] in both directions.
type udpOutbound interface {
	Send(frame []byte)
	// Recv returns the next reply frame, valid until the following call.
	Recv() ([]byte, error)
	Close() error
}

// serveUDPRelay runs the server end of one UDP association until the stream
// closes. With proxy set, datagrams go through its UDP ASSOCIATE.
func serveUDPRelay(id int64, stream net.Conn, proxy *ProxyConfig) {
	var (
		out udpOutbound
		err error
		via string
	)
	if proxy != nil {
		via = " via " + proxy.addr
		var u *socks5UDP
		if u, err = dialSocks5UDP(proxy); err == nil {
			out = u
			via += " (relay " + u.String() + ")"
		}
	} else {
		out, err = newDirectUDP()
	}
	if err != nil {
		log.Printf("[s%d] UDP relay unavailable%s: %v", id, via, err)
		return
	}
	defer out.Close()
	log.Printf("[s%d] UDP relay opened%s", id, via)

	// stream → internet
	go func() {
		defer out.Close() // unblocks Recv below
		buf := make([]byte, maxUDPFrame)
		for {
			n, err := readUDPFrame(stream, buf)
			if err != nil {
				return
			}
			out.Send(buf[:n])
		}
	}()

	// internet → stream
	for {
		frame, err := out.Recv()
		if err != nil {
			break
		}
		if err := writeUDPFrame(stream, frame); err != nil {
			break
		}
	}
	log.Printf("[s%d] UDP relay closed", id)
}

// directUDP sends from one local UDP socket and forwards replies from any peer.
type directUDP struct {
	pc  *net.UDPConn
	res *udpResolver
	buf []byte
}

func newDirectUDP() (*directUDP, error) {
	pc, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	return &directUDP{pc: pc, res: newUDPResolver(), buf: make([]byte, maxUDPFrame)}, nil
}

func (d *directUDP) Send(frame []byte) {
	host, port, off, ok := parseSocksAddr(frame)
	if !ok {
		return
	}
	ip, err := d.res.resolve(host)
	if err != nil {
		return
	}
	d.pc.WriteToUDPAddrPort(frame[off:], netip.AddrPortFrom(ip, port))
}

func (d *directUDP) Recv() ([]byte, error) {
	n, from, err := d.pc.ReadFromUDPAddrPort(d.buf)
	if err != nil {
		return nil, err
	}
	frame := appendSocksAddr(nil, d.res.name(from.Addr().Unmap()), from.Port())
	return append(frame, d.buf[:n]...), nil
}

func (d *directUDP) Close() error { return d.pc.Close() }

// socks5UDP relays through an upstream SOCKS5 proxy's UDP ASSOCIATE
// (RFC 1928 §7). The association lives as long as the TCP control connection.
type socks5UDP struct {
	ctrl net.Conn
	pc   *net.UDPConn // connected to the proxy's UDP relay
	buf  []byte
}

func dialSocks5UDP(proxy *ProxyConfig) (*socks5UDP, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ctrl, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxy.addr)
	if err != nil {
		return nil, fmt.Errorf("connect to SOCKS5 proxy %s: %w", proxy.addr, err)
	}
	ctrl.SetDeadline(time.Now().Add(15 * time.Second))
	if err := socks5ClientAuth(ctrl, proxy.user, proxy.pass); err != nil {
		ctrl.Close()
		return nil, err
	}
	// We don't know which address we'll send from, so ask for any (0.0.0.0:0).
	bndHost, bndPort, err := socks5ClientRequest(ctrl, 0x03, []byte{0x01, 0, 0, 0, 0, 0, 0})
	if err != nil {
		ctrl.Close()
		return nil, err
	}
	ctrl.SetDeadline(time.Time{})

	relay := udpRelayAddr(bndHost, bndPort, ctrl.RemoteAddr().(*net.TCPAddr).AddrPort().Addr())
	pc, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(relay))
	if err != nil {
		ctrl.Close()
		return nil, err
	}

	u := &socks5UDP{ctrl: ctrl, pc: pc, buf: make([]byte, 3+maxUDPFrame)}
	// The proxy ends the association by closing the control connection.
	go func() {
		io.Copy(io.Discard, ctrl)
		pc.Close()
	}()
	return u, nil
}

// udpRelayAddr picks where to send datagrams for a UDP ASSOCIATE reply of
// bndHost:bndPort from a proxy reached at proxyIP. Proxies often report an
// address that only makes sense on their own host — 0.0.0.0 when listening on
// all interfaces, 127.0.0.1 by default in xray — or a hostname; the relay is
// then assumed to be at the proxy's own address.
func udpRelayAddr(bndHost string, bndPort uint16, proxyIP netip.Addr) netip.AddrPort {
	proxyIP = proxyIP.Unmap()
	ip, err := netip.ParseAddr(bndHost)
	if err != nil || ip.IsUnspecified() || (ip.IsLoopback() && !proxyIP.IsLoopback()) {
		ip = proxyIP
	}
	return netip.AddrPortFrom(ip.Unmap(), bndPort)
}

// String names the relay in logs.
func (u *socks5UDP) String() string { return u.pc.RemoteAddr().String() }

func (u *socks5UDP) Send(frame []byte) {
	// Our frames already are SOCKS5 UDP datagrams minus RSV/FRAG.
	pkt := make([]byte, 3+len(frame))
	copy(pkt[3:], frame)
	u.pc.Write(pkt)
}

func (u *socks5UDP) Recv() ([]byte, error) {
	for {
		n, err := u.pc.Read(u.buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil, err
			}
			continue // e.g. ECONNREFUSED from an ICMP error on the connected socket
		}
		if n < 4 || u.buf[2] != 0 { // malformed or fragmented
			continue
		}
		return u.buf[3:n], nil
	}
}

func (u *socks5UDP) Close() error {
	u.ctrl.Close()
	return u.pc.Close()
}

// udpResolver caches hostname lookups for one association and remembers which
// hostname each resolved IP came from, so replies carry the address the client
// actually sent to.
type udpResolver struct {
	mu    sync.Mutex
	fwd   map[string]udpResolved
	names map[netip.Addr]string
}

type udpResolved struct {
	ip      netip.Addr
	expires time.Time
}

func newUDPResolver() *udpResolver {
	return &udpResolver{
		fwd:   make(map[string]udpResolved),
		names: make(map[netip.Addr]string),
	}
}

// resolve returns an IP for host, preferring IPv4 (see dialTarget).
func (r *udpResolver) resolve(host string) (netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Unmap(), nil
	}

	r.mu.Lock()
	e, ok := r.fwd[host]
	r.mu.Unlock()
	if ok && time.Now().Before(e.expires) {
		return e.ip, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), udpResolveTimeout)
	defer cancel()
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return netip.Addr{}, err
	}
	if len(ips) == 0 {
		return netip.Addr{}, fmt.Errorf("no addresses for %s", host)
	}
	ip := ips[0].Unmap()
	for _, a := range ips {
		if a.Unmap().Is4() {
			ip = a.Unmap()
			break
		}
	}

	r.mu.Lock()
	if len(r.fwd) >= udpResolveMax {
		r.fwd = make(map[string]udpResolved)
		r.names = make(map[netip.Addr]string)
	}
	r.fwd[host] = udpResolved{ip: ip, expires: time.Now().Add(udpResolveTTL)}
	r.names[ip] = host
	r.mu.Unlock()
	return ip, nil
}

// name returns the hostname ip was resolved from, or ip itself.
func (r *udpResolver) name(ip netip.Addr) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.names[ip]; ok {
		return h
	}
	return ip.String()
}
