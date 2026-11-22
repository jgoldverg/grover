package udpdataplane

import (
	"context"
)

type NoopPacer struct{}

func (NoopPacer) Wait(context.Context, int) error { return nil }

func (NoopPacer) OnPacketSent(int) {}
