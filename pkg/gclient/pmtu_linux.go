//go:build linux

package gclient

import (
	"errors"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func enablePMTUDiscovery(conn *net.UDPConn, addr *net.UDPAddr) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var sockErr error
	controlErr := raw.Control(func(fd uintptr) {
		if addr != nil && addr.IP.To4() == nil {
			sockErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_MTU_DISCOVER, unix.IPV6_PMTUDISC_DO)
			return
		}
		sockErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_MTU_DISCOVER, unix.IP_PMTUDISC_DO)
	})
	if controlErr != nil {
		return controlErr
	}
	return sockErr
}

func isPMTUTooLarge(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EMSGSIZE) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.EMSGSIZE) {
			return true
		}
	}
	var pathErr *os.SyscallError
	if errors.As(err, &pathErr) {
		return errors.Is(pathErr.Err, syscall.EMSGSIZE)
	}
	return false
}
