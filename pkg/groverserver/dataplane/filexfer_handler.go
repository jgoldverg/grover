package dataplane

import (
	"context"
	"net"
	//"github.com/jgoldverg/grover/pkg/udpwire" // wire structs/parsers live here
)

type FileXferHandler struct{}

func NewFileXferHandler() *FileXferHandler { return &FileXferHandler{} }

func (h *FileXferHandler) OnStart(ctx context.Context, pc net.PacketConn) error { return nil }
func (h *FileXferHandler) OnStop(ctx context.Context, pc net.PacketConn) error  { return nil }

func (h *FileXferHandler) HandlePacket(ctx context.Context, pc net.PacketConn, src net.Addr, buf []byte, n int) {
	//_ = udpwire.Parse(buf[:n]) // TODO: decode header/chunk, route to session, send ACKs, etc.
}
