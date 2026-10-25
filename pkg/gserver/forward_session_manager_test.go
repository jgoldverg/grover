package gserver

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jgoldverg/grover/internal"
	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

func TestForwardSessionManagerLifecycle(t *testing.T) {
	egress := startTCPEchoServer(t)
	manager := newTestForwardSessionManager(t)
	defer manager.Close()

	forward, err := manager.Create(context.Background(), &pb.CreateForwardRequest{
		RouteId:    "route-1",
		JobId:      "job-1",
		HopIndex:   2,
		Protocol:   pb.DataProtocol_DATA_PROTOCOL_TCP,
		Egress:     endpointFromAddr(t, egress.Addr().String()),
		TtlSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if forward.GetForwardId() == "" {
		t.Fatal("forward id is empty")
	}
	if forward.GetIngress().GetPort() == 0 {
		t.Fatalf("ingress not allocated: %+v", forward.GetIngress())
	}

	got, err := manager.Get(forward.GetForwardId())
	if err != nil {
		t.Fatal(err)
	}
	if got.GetRouteId() != "route-1" || got.GetJobId() != "job-1" || got.GetHopIndex() != 2 {
		t.Fatalf("unexpected forward metadata: %+v", got)
	}
	if list := manager.List("route-1", "job-1"); len(list) != 1 {
		t.Fatalf("list count = %d, want 1", len(list))
	}
	renewed, err := manager.Renew(forward.GetForwardId(), 60)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.GetTtlSeconds() != 60 {
		t.Fatalf("ttl = %d, want 60", renewed.GetTtlSeconds())
	}
	if ok := manager.Delete(forward.GetForwardId()); !ok {
		t.Fatal("delete returned false")
	}
	if _, err := manager.Get(forward.GetForwardId()); err == nil {
		t.Fatal("expected deleted forward to be missing")
	}
}

func TestForwardSessionManagerCloseIsIdempotent(t *testing.T) {
	manager := newTestForwardSessionManager(t)
	manager.Close()
	manager.Close()
}

func TestForwardSessionManagerTCPForwarding(t *testing.T) {
	egress := startTCPEchoServer(t)
	manager := newTestForwardSessionManager(t)
	defer manager.Close()

	forward, err := manager.Create(context.Background(), &pb.CreateForwardRequest{
		RouteId:  "route-1",
		JobId:    "job-1",
		Protocol: pb.DataProtocol_DATA_PROTOCOL_TCP,
		Egress:   endpointFromAddr(t, egress.Addr().String()),
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", forward.GetIngress().GetPort())))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "hello\n"); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "echo:hello\n" {
		t.Fatalf("line = %q, want echo:hello", line)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot, err := manager.Get(forward.GetForwardId())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.GetStats().GetIngressBytes() > 0 && snapshot.GetStats().GetEgressBytes() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats did not update: %+v", snapshot.GetStats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestForwardSessionManagerUDPForwarding(t *testing.T) {
	egress := startUDPEchoServer(t)
	manager := newTestForwardSessionManager(t)
	defer manager.Close()

	forward, err := manager.Create(context.Background(), &pb.CreateForwardRequest{
		RouteId:  "route-udp",
		JobId:    "job-udp",
		Protocol: pb.DataProtocol_DATA_PROTOCOL_UDP,
		Egress:   endpointFromAddr(t, egress.LocalAddr().String()),
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("udp", net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", forward.GetIngress().GetPort())))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "echo:ping" {
		t.Fatalf("udp response = %q, want echo:ping", got)
	}
}

func TestForwardSessionManagerExpiry(t *testing.T) {
	egress := startTCPEchoServer(t)
	manager := newTestForwardSessionManager(t)
	defer manager.Close()

	forward, err := manager.Create(context.Background(), &pb.CreateForwardRequest{
		Protocol:   pb.DataProtocol_DATA_PROTOCOL_TCP,
		Egress:     endpointFromAddr(t, egress.Addr().String()),
		TtlSeconds: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	manager.expireNow(time.Now().Add(2 * time.Second))
	if _, err := manager.Get(forward.GetForwardId()); err == nil {
		t.Fatal("expected expired forward to be removed")
	}
}

func newTestForwardSessionManager(t *testing.T) *ForwardSessionManager {
	t.Helper()
	manager, err := NewForwardSessionManager(&internal.ServerConfig{
		DataBindHost:      "127.0.0.1",
		DataAdvertiseHost: "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func startTCPEchoServer(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					_, _ = io.WriteString(c, "echo:"+scanner.Text()+"\n")
				}
			}(conn)
		}
	}()
	return listener
}

func startUDPEchoServer(t *testing.T) *net.UDPConn {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP([]byte("echo:"+string(buf[:n])), addr)
		}
	}()
	return conn
}

func endpointFromAddr(t *testing.T, addr string) *pb.DataEndpoint {
	t.Helper()
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var port uint32
	if _, err := fmt.Sscanf(portText, "%d", &port); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
	}
	return &pb.DataEndpoint{Host: host, Port: port}
}
