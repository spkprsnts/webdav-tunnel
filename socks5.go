package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
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
