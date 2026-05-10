//go:build !linux

package gclient

import (
	"net"
)

func enablePMTUDiscovery(*net.UDPConn, *net.UDPAddr) error {
	return ErrPMTUUnsupported
}

func isPMTUTooLarge(error) bool {
	return false
}
