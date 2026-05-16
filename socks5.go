package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
)

const socks5Version = 0x05

// Handshake выполняет SOCKS5-рукопожатие и возвращает целевой host и port.
// Поддерживает IPv4, IPv6 и доменные имена (SOCKS5h — резолвинг на сервере).
// После успешного возврата соединение готово к передаче данных.
func socks5Handshake(conn net.Conn) (host string, port uint16, err error) {
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
	// выбираем «без авторизации»
	if _, err = conn.Write([]byte{socks5Version, 0x00}); err != nil {
		return
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
	if req[1] != 0x01 { // CONNECT only
		// отправляем «command not supported»
		conn.Write([]byte{socks5Version, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		err = fmt.Errorf("unsupported SOCKS5 command 0x%02x", req[1])
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

	case 0x03: // domain (SOCKS5h: резолвинг на удалённой стороне)
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

// dialViaSocks5 подключается к targetHost:targetPort через SOCKS5-прокси.
// DNS-резолвинг происходит на стороне прокси (SOCKS5h).
func dialViaSocks5(ctx context.Context, proxy *proxyConfig, targetHost, targetPort string) (net.Conn, error) {
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

// socks5Connect выполняет клиентский SOCKS5-хендшейк и команду CONNECT.
func socks5Connect(conn net.Conn, user, pass, host, port string) error {
	// greeting — предлагаем no-auth, и user/pass если есть учётные данные
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

	// CONNECT request — передаём hostname (ATYP=0x03), резолвинг на прокси
	portNum, _ := strconv.Atoi(port)
	req := make([]byte, 0, 7+len(host))
	req = append(req, 0x05, 0x01, 0x00, 0x03, byte(len(host)))
	req = append(req, host...)
	req = append(req, byte(portNum>>8), byte(portNum&0xff))
	if _, err := conn.Write(req); err != nil {
		return err
	}

	// ответ: VER REP RSV ATYP
	var rep [4]byte
	if _, err := io.ReadFull(conn, rep[:]); err != nil {
		return fmt.Errorf("SOCKS5 CONNECT response: %w", err)
	}
	if rep[0] != 0x05 {
		return fmt.Errorf("SOCKS5: unexpected version in CONNECT response")
	}
	if rep[1] != 0x00 {
		return fmt.Errorf("SOCKS5 CONNECT rejected: code 0x%02x", rep[1])
	}

	// вычитываем bound address (нам не нужен, но обязательно читаем)
	switch rep[3] {
	case 0x01:
		var buf [4]byte
		_, err := io.ReadFull(conn, buf[:])
		if err != nil {
			return err
		}
	case 0x03:
		var l [1]byte
		if _, err := io.ReadFull(conn, l[:]); err != nil {
			return err
		}
		if _, err := io.ReadFull(conn, make([]byte, l[0])); err != nil {
			return err
		}
	case 0x04:
		var buf [16]byte
		if _, err := io.ReadFull(conn, buf[:]); err != nil {
			return err
		}
	default:
		return fmt.Errorf("SOCKS5: unknown address type 0x%02x in response", rep[3])
	}
	var pbuf [2]byte
	if _, err := io.ReadFull(conn, pbuf[:]); err != nil {
		return err
	}
	return nil
}
