package tunnel

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// dialViaSocks5 must still handshake, send the hostname for remote DNS, and
// accept a bound address of any type in the reply.
func TestDialViaSocks5Connect(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	go func() {
		c, err := target.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		io.Copy(c, c) // echo
	}()

	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	gotHost := make(chan string, 1)
	go func() {
		c, err := proxy.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		io.ReadFull(c, make([]byte, 3)) // VER NMETHODS METHOD(no auth)
		c.Write([]byte{0x05, 0x00})
		var req [5]byte // VER CMD RSV ATYP LEN
		io.ReadFull(c, req[:])
		name := make([]byte, req[4])
		io.ReadFull(c, name)
		io.ReadFull(c, make([]byte, 2))
		gotHost <- string(name)
		// Reply with a domain-type bound address.
		c.Write(append([]byte{0x05, 0x00, 0x00, 0x03, 5}, "proxy\x00\x00"...))
		up, err := net.Dial("tcp", target.Addr().String())
		if err != nil {
			return
		}
		relayStreams(c, up)
	}()

	_, port, _ := net.SplitHostPort(target.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialViaSocks5(ctx, NewProxyConfig(proxy.Addr().String(), "", ""), "localhost", port)
	if err != nil {
		t.Fatalf("dialViaSocks5: %v", err)
	}
	defer conn.Close()
	if h := <-gotHost; h != "localhost" {
		t.Errorf("proxy got host %q, want the hostname for remote DNS", h)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	conn.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo through proxy: %q, %v", buf, err)
	}
}
