package udpdataplane

type FixedWindowFlowController struct {
	maxPackets int
	maxBytes   int64
}

func newFixedWindowFlowController(cfg SendConfig, payloadSize int, windowPackets int) FixedWindowFlowController {
	maxBytes := int64(0)
	if cfg.WindowBytes > 0 {
		maxBytes = int64(cfg.WindowBytes)
	} else if payloadSize > 0 && windowPackets > 0 {
		maxBytes = int64(payloadSize * windowPackets)
	}
	return FixedWindowFlowController{maxPackets: windowPackets, maxBytes: maxBytes}
}

func (c FixedWindowFlowController) CanSend(state SendState) bool {
	if c.maxPackets > 0 && state.InflightPackets+state.BatchPackets >= c.maxPackets {
		return false
	}
	if c.maxBytes > 0 && state.InflightBytes+state.BatchBytes >= c.maxBytes {
		return false
	}
	return true
}

type BBRFlowController struct {
	maxBytes int64
}

func newBBRFlowController(cfg SendConfig) BBRFlowController {
	return BBRFlowController{maxBytes: int64(cfg.WindowBytes)}
}

func (c BBRFlowController) CanSend(state SendState) bool {
	if c.maxBytes > 0 && state.InflightBytes+state.BatchBytes >= c.maxBytes {
		return false
	}
	return true
}
