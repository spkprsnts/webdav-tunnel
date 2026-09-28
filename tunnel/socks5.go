package tunnel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
)

const socks5Version = 0x05

// socks5Handshake performs the SOCKS5 server-side handshake and returns the
// command byte (0x01 CONNECT, 0x03 UDP ASSOCIATE), target host, and port.
// Supports IPv4, IPv6, and domain names (SOCKS5h).
// If user != "", requires username/password authentication (RFC 1929).
func socks5Handshake(conn net.Conn, user, pass string) (cmd byte, host string, port uint16, err error) {
	// --- greeting ---
	hdr := make([]byte, 2)
	if _, err = io.ReadFull(conn, hdr); err != nil {
		return
	}
	if hdr[0] != socks5Version {
		err = fmt.Errorf("unsupported SOCKS version %d", hdr[0])
		return
	}
	methods := make([]byte, hdr[1])
	if _, err = io.ReadFull(conn, methods); err != nil {
		return
	}

	// --- method selection ---
	wantAuth := user != ""
	var selected byte = 0xFF
	for _, m := range methods {
		if !wantAuth && m == 0x00 {
			selected = 0x00
			break
		}
		if wantAuth && m == 0x02 {
			selected = 0x02
			break
		}
	}
	if _, err = conn.Write([]byte{socks5Version, selected}); err != nil {
		return
	}
	if selected == 0xFF {
		err = fmt.Errorf("client offered no acceptable auth method")
		return
	}

	// --- RFC 1929: username/password sub-negotiation ---
	if selected == 0x02 {
		var subHdr [2]byte // VER(1) ULEN(1)
		if _, err = io.ReadFull(conn, subHdr[:]); err != nil {
			return
		}
		uname := make([]byte, subHdr[1])
		if _, err = io.ReadFull(conn, uname); err != nil {
			return
		}
		var plenBuf [1]byte
		if _, err = io.ReadFull(conn, plenBuf[:]); err != nil {
			return
		}
		passwd := make([]byte, plenBuf[0])
		if _, err = io.ReadFull(conn, passwd); err != nil {
			return
		}
		if string(uname) != user || string(passwd) != pass {
			conn.Write([]byte{0x01, 0x01}) // auth failure
			err = fmt.Errorf("SOCKS5 authentication failed")
			return
		}
		if _, err = conn.Write([]byte{0x01, 0x00}); err != nil { // auth success
			return
		}
	}

	// --- request ---
	req := make([]byte, 4)
	if _, err = io.ReadFull(conn, req); err != nil {
		return
	}
	if req[0] != socks5Version {
		err = fmt.Errorf("bad SOCKS5 request version")
		return
	}
	cmd = req[1]
	if cmd != 0x01 && cmd != 0x03 { // CONNECT or UDP ASSOCIATE
		conn.Write([]byte{socks5Version, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		err = fmt.Errorf("unsupported SOCKS5 command 0x%02x", cmd)
		return
	}

	// --- address ---
	switch req[3] {
	case 0x01: // IPv4
		addr := make([]byte, 4)
		if _, err = io.ReadFull(conn, addr); err != nil {
			return
		}
		host = net.IP(addr).String()

	case 0x03: // domain (SOCKS5h: DNS resolved on the remote side)
		lenBuf := make([]byte, 1)
		if _, err = io.ReadFull(conn, lenBuf); err != nil {
			return
		}
		domain := make([]byte, lenBuf[0])
		if _, err = io.ReadFull(conn, domain); err != nil {
			return
		}
		host = string(domain)

	case 0x04: // IPv6
		addr := make([]byte, 16)
		if _, err = io.ReadFull(conn, addr); err != nil {
			return
		}
		host = net.IP(addr).String()

	default:
		conn.Write([]byte{socks5Version, 0x08, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		err = fmt.Errorf("unsupported address type 0x%02x", req[3])
		return
	}

	// --- port ---
	portBuf := make([]byte, 2)
	if _, err = io.ReadFull(conn, portBuf); err != nil {
		return
	}
	port = binary.BigEndian.Uint16(portBuf)

	return
}

// dialViaSocks5 connects to targetHost:targetPort through a SOCKS5 proxy.
// DNS resolution happens on the proxy side (SOCKS5h).
func dialViaSocks5(ctx context.Context, proxy *ProxyConfig, targetHost, targetPort string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxy.addr)
	if err != nil {
		return nil, fmt.Errorf("connect to SOCKS5 proxy %s: %w", proxy.addr, err)
	}
	if err := socks5Connect(conn, proxy.user, proxy.pass, targetHost, targetPort); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// socks5Connect performs the client-side SOCKS5 handshake and sends a CONNECT command.
func socks5Connect(conn net.Conn, user, pass, host, port string) error {
	if err := socks5ClientAuth(conn, user, pass); err != nil {
		return err
	}
	// Send the hostname (ATYP=0x03) so DNS is resolved on the proxy.
	portNum, _ := strconv.Atoi(port)
	addr := make([]byte, 0, 4+len(host))
	addr = append(addr, 0x03, byte(len(host)))
	addr = append(addr, host...)
	addr = append(addr, byte(portNum>>8), byte(portNum&0xff))
	_, _, err := socks5ClientRequest(conn, 0x01, addr)
	return err
}

// socks5ClientAuth performs the client-side greeting and, if the proxy asks
// for it, RFC 1929 username/password authentication.
func socks5ClientAuth(conn net.Conn, user, pass string) error {
	// Offer no-auth; also offer username/password if credentials are provided.
	if user != "" {
		if _, err := conn.Write([]byte{0x05, 0x02, 0x00, 0x02}); err != nil {
			return err
		}
	} else {
		if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
			return err
		}
	}

	var resp [2]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		return fmt.Errorf("SOCKS5 greeting: %w", err)
	}
	if resp[0] != 0x05 {
		return fmt.Errorf("SOCKS5: unexpected version %d from proxy", resp[0])
	}

	switch resp[1] {
	case 0x00: // no auth
	case 0x02: // username/password (RFC 1929)
		if user == "" {
			return fmt.Errorf("SOCKS5 proxy requires authentication")
		}
		auth := make([]byte, 0, 3+len(user)+len(pass))
		auth = append(auth, 0x01, byte(len(user)))
		auth = append(auth, user...)
		auth = append(auth, byte(len(pass)))
		auth = append(auth, pass...)
		if _, err := conn.Write(auth); err != nil {
			return err
		}
		var ar [2]byte
		if _, err := io.ReadFull(conn, ar[:]); err != nil {
			return fmt.Errorf("SOCKS5 auth: %w", err)
		}
		if ar[1] != 0x00 {
			return fmt.Errorf("SOCKS5 authentication failed")
		}
	case 0xFF:
		return fmt.Errorf("SOCKS5: proxy rejected all auth methods")
	default:
		return fmt.Errorf("SOCKS5: unsupported auth method 0x%02x", resp[1])
	}
	return nil
}

// socks5ClientRequest sends command cmd for addr ([ATYP][ADDR][2 PORT]) and
// returns the bound address from the proxy's reply.
func socks5ClientRequest(conn net.Conn, cmd byte, addr []byte) (bndHost string, bndPort uint16, err error) {
	name := map[byte]string{0x01: "CONNECT", 0x03: "UDP ASSOCIATE"}[cmd]
	req := append([]byte{0x05, cmd, 0x00}, addr...)
	if _, err := conn.Write(req); err != nil {
		return "", 0, err
	}

	// response: VER REP RSV ATYP
	var rep [4]byte
	if _, err := io.ReadFull(conn, rep[:]); err != nil {
		return "", 0, fmt.Errorf("SOCKS5 %s response: %w", name, err)
	}
	if rep[0] != 0x05 {
		return "", 0, fmt.Errorf("SOCKS5: unexpected version in %s response", name)
	}
	if rep[1] != 0x00 {
		return "", 0, fmt.Errorf("SOCKS5 %s rejected: code 0x%02x", name, rep[1])
	}

	// The bound address is required by the protocol even when unused.
	var raw []byte
	switch rep[3] {
	case 0x01:
		raw = make([]byte, 1+4+2)
	case 0x03:
		var l [1]byte
		if _, err := io.ReadFull(conn, l[:]); err != nil {
			return "", 0, err
		}
		raw = make([]byte, 2+int(l[0])+2)
		raw[1] = l[0]
	case 0x04:
		raw = make([]byte, 1+16+2)
	default:
		return "", 0, fmt.Errorf("SOCKS5: unknown address type 0x%02x in response", rep[3])
	}
	raw[0] = rep[3]
	off := 1
	if rep[3] == 0x03 {
		off = 2
	}
	if _, err := io.ReadFull(conn, raw[off:]); err != nil {
		return "", 0, err
	}
	bndHost, bndPort, _, _ = parseSocksAddr(raw)
	return bndHost, bndPort, nil
}
