package tunnel

import (
	"context"
	"encoding/binary"
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

// serveUDPRelay runs the server end of one UDP association until the stream
// closes.
func serveUDPRelay(id int64, stream net.Conn) {
	pc, err := net.ListenUDP("udp", nil)
	if err != nil {
		log.Printf("[s%d] UDP relay listen failed: %v", id, err)
		return
	}
	defer pc.Close()
	log.Printf("[s%d] UDP relay opened", id)

	res := newUDPResolver()

	// stream → internet
	go func() {
		defer pc.Close() // unblocks the reader below
		buf := make([]byte, maxUDPFrame)
		for {
			n, err := readUDPFrame(stream, buf)
			if err != nil {
				return
			}
			host, port, off, ok := parseSocksAddr(buf[:n])
			if !ok {
				continue
			}
			ip, err := res.resolve(host)
			if err != nil {
				continue
			}
			pc.WriteToUDPAddrPort(buf[off:n], netip.AddrPortFrom(ip, port))
		}
	}()

	// internet → stream
	buf := make([]byte, maxUDPFrame)
	for {
		n, from, err := pc.ReadFromUDPAddrPort(buf)
		if err != nil {
			break
		}
		frame := appendSocksAddr(nil, res.name(from.Addr().Unmap()), from.Port())
		frame = append(frame, buf[:n]...)
		if err := writeUDPFrame(stream, frame); err != nil {
			break
		}
	}
	log.Printf("[s%d] UDP relay closed", id)
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
