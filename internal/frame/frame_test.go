package frame

import (
	"bytes"
	"errors"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	input := append(Encode(Open, MaxStreamID, nil), Encode(Data, 7, []byte("payload"))...)
	frames, err := ParseAll(input, MaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || frames[0].StreamID != MaxStreamID || !bytes.Equal(frames[1].Payload, []byte("payload")) {
		t.Fatalf("unexpected frames: %#v", frames)
	}
}

func TestRejectsPartialAndOversized(t *testing.T) {
	if _, err := ParseAll(Encode(Data, 1, []byte("x"))[:8], MaxPayload); !errors.Is(err, ErrIncomplete) {
		t.Fatalf("expected incomplete frame, got %v", err)
	}
	if _, err := ParseAll(Encode(Data, 1, []byte("xx")), 1); !errors.Is(err, ErrPayload) {
		t.Fatalf("expected oversized payload, got %v", err)
	}
}

func TestHelloAndWindow(t *testing.T) {
	if err := ParseHello(Encode(Hello, 0, []byte{1})); err != nil {
		t.Fatal(err)
	}
	if err := ParseHello(Encode(Hello, 0, []byte{2})); err == nil {
		t.Fatal("accepted unsupported protocol version")
	}
	if _, err := WindowAmount([]byte{0, 0, 0, 0}); err == nil {
		t.Fatal("accepted zero window")
	}
}

func TestRejectsExcessiveFrameCount(t *testing.T) {
	input := bytes.Repeat(Encode(Close, 1, nil), MaxBatchFrames+1)
	if _, err := ParseAll(input, MaxPayload); err == nil {
		t.Fatal("accepted excessive frame count")
	}
}

func TestOpenPayloadShape(t *testing.T) {
	destination := []byte("example.com:443")
	if err := ValidateClientShape(Frame{Type: Open, StreamID: 1, Payload: destination}, true); err != nil {
		t.Fatalf("a tunnel profile rejected a destination: %v", err)
	}
	if err := ValidateClientShape(Frame{Type: Open, StreamID: 1}, true); err != nil {
		t.Fatalf("a tunnel profile rejected an empty OPEN: %v", err)
	}
	if err := ValidateClientShape(Frame{Type: Open, StreamID: 1, Payload: destination}, false); err == nil {
		t.Fatal("an MTProxy profile accepted a destination in OPEN")
	}
	if err := ValidateClientShape(Frame{Type: Open, StreamID: 1, Payload: make([]byte, MaxOpenPayload+1)}, true); err == nil {
		t.Fatal("an oversized OPEN payload was accepted")
	}
	// CLOSE stays empty for every profile, and the other types are unchanged.
	if err := ValidateClientShape(Frame{Type: Close, StreamID: 1, Payload: destination}, true); err == nil {
		t.Fatal("a CLOSE payload was accepted")
	}
	if err := ValidateClientShape(Frame{Type: Data, StreamID: 1, Payload: []byte("x")}, true); err != nil {
		t.Fatalf("DATA was rejected: %v", err)
	}
	if err := ValidateClientShape(Frame{Type: Data, StreamID: 1}, true); err == nil {
		t.Fatal("an empty DATA payload was accepted")
	}
}
