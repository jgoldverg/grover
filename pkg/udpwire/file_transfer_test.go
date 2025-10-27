package udpwire

import "testing"

func TestStatusPacketEncodeDecodeWithSacks(t *testing.T) {
	original := StatusPacket{
		SessionID: 0xdeadbeef,
		StreamID:  0x01020304,
		AckSeq:    512,
		Sacks: []SackRange{
			{Start: 10, End: 14},
			{Start: 21, End: 22},
		},
	}

	buf := make([]byte, StatusHeaderLen+len(original.Sacks)*SackBlockLen)
	n, err := original.Encode(buf)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var decoded StatusPacket
	read, err := decoded.Decode(buf[:n])
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if read != n {
		t.Fatalf("expected decode to consume %d bytes, got %d", n, read)
	}

	if decoded.SessionID != original.SessionID {
		t.Fatalf("session mismatch: got %d want %d", decoded.SessionID, original.SessionID)
	}
	if decoded.StreamID != original.StreamID {
		t.Fatalf("stream mismatch: got %d want %d", decoded.StreamID, original.StreamID)
	}
	if decoded.AckSeq != original.AckSeq {
		t.Fatalf("ack mismatch: got %d want %d", decoded.AckSeq, original.AckSeq)
	}
	if len(decoded.Sacks) != len(original.Sacks) {
		t.Fatalf("sack length mismatch: got %d want %d", len(decoded.Sacks), len(original.Sacks))
	}
	for i, rng := range decoded.Sacks {
		if rng != original.Sacks[i] {
			t.Fatalf("sack[%d] mismatch: got %+v want %+v", i, rng, original.Sacks[i])
		}
	}
}

func TestStatusPacketEncodeBufferTooSmall(t *testing.T) {
	sp := StatusPacket{
		SessionID: 1,
		StreamID:  2,
		AckSeq:    3,
		Sacks:     []SackRange{{Start: 1, End: 2}},
	}
	buf := make([]byte, StatusHeaderLen-1)
	if _, err := sp.Encode(buf); err == nil {
		t.Fatal("expected encode error for short buffer")
	}
}

func TestStatusPacketDecodeTruncated(t *testing.T) {
	sp := StatusPacket{SessionID: 1, StreamID: 2, AckSeq: 3}
	buf := make([]byte, StatusHeaderLen)
	if _, err := sp.Encode(buf); err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	var out StatusPacket
	if _, err := out.Decode(buf[:StatusHeaderLen-1]); err == nil {
		t.Fatal("expected decode error for truncated packet")
	}
}
