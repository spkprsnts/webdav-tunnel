package tunnel

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

func TestSocksAddrRoundTrip(t *testing.T) {
	cases := []struct {
		host string
		port uint16
	}{
		{"1.2.3.4", 53},
		{"2001:db8::1", 443},
		{"example.com", 8080},
	}
	for _, c := range cases {
		b := appendSocksAddr(nil, c.host, c.port)
		b = append(b, 0xAA) // trailing payload must not be consumed
		host, port, n, ok := parseSocksAddr(b)
		if !ok || host != c.host || port != c.port || n != len(b)-1 {
			t.Errorf("%s:%d → %q:%d n=%d ok=%v", c.host, c.port, host, port, n, ok)
		}
	}
	for _, bad := range [][]byte{nil, {0x01, 1, 2}, {0x03, 5, 'a'}, {0x09, 0, 0}} {
		if _, _, _, ok := parseSocksAddr(bad); ok {
			t.Errorf("parseSocksAddr(%v) accepted malformed input", bad)
		}
	}
}

// startUDPEcho starts a UDP server on 127.0.0.1 that echoes every datagram.
func startUDPEcho(t *testing.T) *net.UDPAddr {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	go func() {
		buf := make([]byte, 65536)
		for {
			n, src, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			pc.WriteTo(buf[:n], src)
		}
	}()
	return pc.LocalAddr().(*net.UDPAddr)
}

// startMuxedSocks wires a SOCKS5 listener → yamux client ⇄ yamux server →
// serverStream over an in-memory pipe, and returns the SOCKS5 address.
func startMuxedSocks(t *testing.T, proxy *ProxyConfig) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	a, b := net.Pipe()
	cli, err := yamux.Client(a, yamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := yamux.Server(b, yamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close(); srv.Close() })
	go func() {
		for {
			s, err := srv.Accept()
			if err != nil {
				return
			}
			go serverStream(s, proxy)
		}
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go proxyStream(ctx, cli, c, nil, "", "")
		}
	}()
	return ln.Addr().String()
}

// udpAssociate performs a no-auth SOCKS5 UDP ASSOCIATE and returns the control
// connection and a UDP socket connected to the relay.
func udpAssociate(t *testing.T, socksAddr string) (net.Conn, net.Conn) {
	t.Helper()
	ctrl, err := net.Dial("tcp", socksAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ctrl.Close() })
	ctrl.SetDeadline(time.Now().Add(5 * time.Second))

	ctrl.Write([]byte{0x05, 0x01, 0x00})
	var sel [2]byte
	if _, err := io.ReadFull(ctrl, sel[:]); err != nil || sel[1] != 0x00 {
		t.Fatalf("method selection: %v %v", sel, err)
	}
	ctrl.Write([]byte{0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	var rep [10]byte
	if _, err := io.ReadFull(ctrl, rep[:]); err != nil || rep[1] != 0x00 {
		t.Fatalf("UDP ASSOCIATE reply: %v %v", rep, err)
	}
	relay := &net.UDPAddr{IP: net.IP(rep[4:8]), Port: int(binary.BigEndian.Uint16(rep[8:10]))}

	uc, err := net.DialUDP("udp", nil, relay)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { uc.Close() })
	return ctrl, uc
}

// udpRoundTrip sends payload to host:port through the relay and returns the
// reply's source address and payload.
func udpRoundTrip(t *testing.T, uc net.Conn, host string, port uint16, payload []byte) (string, uint16, []byte, error) {
	t.Helper()
	pkt := appendSocksAddr([]byte{0, 0, 0}, host, port)
	pkt = append(pkt, payload...)
	uc.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := uc.Write(pkt); err != nil {
		return "", 0, nil, err
	}
	buf := make([]byte, 65536)
	n, err := uc.Read(buf)
	if err != nil {
		return "", 0, nil, err
	}
	h, p, off, ok := parseSocksAddr(buf[3:n])
	if !ok {
		t.Fatalf("malformed reply header: %v", buf[:n])
	}
	return h, p, buf[3+off : n], nil
}

func TestUDPRelayEcho(t *testing.T) {
	echo := startUDPEcho(t)
	_, uc := udpAssociate(t, startMuxedSocks(t, nil))

	for i := 0; i < 3; i++ {
		payload := []byte("ping-" + strconv.Itoa(i))
		h, p, got, err := udpRoundTrip(t, uc, "127.0.0.1", uint16(echo.Port), payload)
		if err != nil {
			t.Fatalf("round trip %d: %v", i, err)
		}
		if h != "127.0.0.1" || int(p) != echo.Port || !bytes.Equal(got, payload) {
			t.Fatalf("round trip %d: got %s:%d %q", i, h, p, got)
		}
	}
}

// A datagram sent to a hostname comes back with that hostname as its source,
// not the IP the server resolved it to.
func TestUDPRelayDomainSource(t *testing.T) {
	echo := startUDPEcho(t)
	_, uc := udpAssociate(t, startMuxedSocks(t, nil))

	h, p, got, err := udpRoundTrip(t, uc, "localhost", uint16(echo.Port), []byte("hi"))
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if h != "localhost" || int(p) != echo.Port || string(got) != "hi" {
		t.Fatalf("got %s:%d %q, want localhost:%d \"hi\"", h, p, got, echo.Port)
	}
}

// startFakeSocks5UDP starts a minimal SOCKS5 proxy that requires user/pass
// and implements only UDP ASSOCIATE — or refuses it, like Tor or ssh -D, when
// allowUDP is false. It reports its relay as 0.0.0.0, as proxies listening on
// all interfaces often do. relayed counts datagrams it forwarded outwards.
func startFakeSocks5UDP(t *testing.T, user, pass string, allowUDP bool) (addr string, relayed *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	relayed = new(atomic.Int64)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go fakeSocks5UDPConn(c, user, pass, allowUDP, relayed)
		}
	}()
	return ln.Addr().String(), relayed
}

func fakeSocks5UDPConn(c net.Conn, user, pass string, allowUDP bool, relayed *atomic.Int64) {
	defer c.Close()
	var hdr [2]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return
	}
	io.ReadFull(c, make([]byte, hdr[1])) // offered methods
	c.Write([]byte{0x05, 0x02})
	var ver [2]byte
	io.ReadFull(c, ver[:])
	u := make([]byte, ver[1])
	io.ReadFull(c, u)
	var pl [1]byte
	io.ReadFull(c, pl[:])
	p := make([]byte, pl[0])
	io.ReadFull(c, p)
	if string(u) != user || string(p) != pass {
		c.Write([]byte{0x01, 0x01})
		return
	}
	c.Write([]byte{0x01, 0x00})

	var req [4]byte
	if _, err := io.ReadFull(c, req[:]); err != nil {
		return
	}
	switch req[3] { // skip DST.ADDR + DST.PORT
	case 0x01:
		io.ReadFull(c, make([]byte, 6))
	case 0x04:
		io.ReadFull(c, make([]byte, 18))
	case 0x03:
		var l [1]byte
		io.ReadFull(c, l[:])
		io.ReadFull(c, make([]byte, int(l[0])+2))
	}
	if req[1] != 0x03 || !allowUDP {
		c.Write([]byte{0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0}) // command not supported
		return
	}

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return
	}
	defer pc.Close()
	rep := []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint16(rep[8:], uint16(pc.LocalAddr().(*net.UDPAddr).Port))
	c.Write(rep)

	go func() {
		var client net.Addr
		buf := make([]byte, 65536)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if client == nil {
				client = from
			}
			if from.String() == client.String() {
				host, port, off, ok := parseSocksAddr(buf[3:n])
				if !ok {
					continue
				}
				dst, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(int(port))))
				if err != nil {
					continue
				}
				relayed.Add(1)
				pc.WriteTo(buf[3+off:n], dst)
				continue
			}
			ua := from.(*net.UDPAddr)
			reply := appendSocksAddr([]byte{0, 0, 0}, ua.IP.String(), uint16(ua.Port))
			pc.WriteTo(append(reply, buf[:n]...), client)
		}
	}()
	io.Copy(io.Discard, c) // the association lasts as long as this connection
}

// With an upstream proxy, UDP goes through its UDP ASSOCIATE instead of
// leaving the server directly.
func TestUDPRelayViaUpstreamProxy(t *testing.T) {
	echo := startUDPEcho(t)
	proxyAddr, relayed := startFakeSocks5UDP(t, "pu", "pp", true)
	_, uc := udpAssociate(t, startMuxedSocks(t, NewProxyConfig(proxyAddr, "pu", "pp")))

	for i := 0; i < 3; i++ {
		payload := []byte("via-proxy-" + strconv.Itoa(i))
		h, p, got, err := udpRoundTrip(t, uc, "127.0.0.1", uint16(echo.Port), payload)
		if err != nil {
			t.Fatalf("round trip %d: %v", i, err)
		}
		if h != "127.0.0.1" || int(p) != echo.Port || !bytes.Equal(got, payload) {
			t.Fatalf("round trip %d: got %s:%d %q", i, h, p, got)
		}
	}
	if n := relayed.Load(); n != 3 {
		t.Errorf("proxy relayed %d datagrams, want 3 (traffic bypassed the proxy?)", n)
	}
}

// An upstream proxy without UDP support (Tor, ssh -D): datagrams are dropped
// rather than sent around the proxy, and the association stays up.
func TestUDPRelayUpstreamWithoutUDP(t *testing.T) {
	echo := startUDPEcho(t)
	proxyAddr, _ := startFakeSocks5UDP(t, "pu", "pp", false)
	ctrl, uc := udpAssociate(t, startMuxedSocks(t, NewProxyConfig(proxyAddr, "pu", "pp")))

	if _, _, _, err := udpRoundTrip(t, uc, "127.0.0.1", uint16(echo.Port), []byte("x")); err == nil {
		t.Fatal("got a reply although the proxy refused UDP")
	}
	// Control connection must still be open.
	ctrl.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := ctrl.Read(make([]byte, 1)); err == nil || !isTimeout(err) {
		t.Fatalf("control connection closed: %v", err)
	}
}

func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}

func TestUDPRelayAddr(t *testing.T) {
	remote := netip.MustParseAddr("203.0.113.7")
	local := netip.MustParseAddr("127.0.0.1")
	cases := []struct {
		bnd   string
		proxy netip.Addr
		want  string
	}{
		{"198.51.100.1", remote, "198.51.100.1:5000"}, // reported address is used as is
		{"0.0.0.0", remote, "203.0.113.7:5000"},       // listening on all interfaces
		{"127.0.0.1", remote, "203.0.113.7:5000"},     // xray's default "ip" on a remote proxy
		{"127.0.0.1", local, "127.0.0.1:5000"},        // a local proxy really is on loopback
		{"proxy.example", remote, "203.0.113.7:5000"}, // hostname
	}
	for _, c := range cases {
		if got := udpRelayAddr(c.bnd, 5000, c.proxy).String(); got != c.want {
			t.Errorf("udpRelayAddr(%s, proxy %s) = %s, want %s", c.bnd, c.proxy, got, c.want)
		}
	}
}
