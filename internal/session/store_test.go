package session

import (
	"errors"
	"testing"

	"github.com/jgoldverg/grover/internal/pb"
)

func validSession(id string) *pb.RouteSession {
	return &pb.RouteSession{
		SessionId:   id,
		RouteId:     "r",
		Data:        &pb.DataEndpoint{Host: "h", Port: 1},
		Destination: &pb.TransferEndpoint{Token: []byte("t")},
	}
}

func TestLifecycle(t *testing.T) {
	s := NewStore()
	if _, err := s.Create(&pb.RouteSession{SessionId: "x"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing data endpoint should be invalid: %v", err)
	}
	if _, err := s.Create(validSession("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(validSession("a")); err == nil {
		t.Fatal("duplicate id should fail")
	}
	if _, err := s.Claim("a", "job1"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("a")
	if got.State != pb.RuntimeState_RUNTIME_STATE_RUNNING || got.ActiveJobId != "job1" {
		t.Fatalf("claimed session view: %v", got)
	}
	if _, err := s.Claim("a", "job2"); !errors.Is(err, ErrBusy) {
		t.Fatalf("second claim: %v", err)
	}
	if err := s.Delete("a"); !errors.Is(err, ErrBusy) {
		t.Fatalf("delete while busy: %v", err)
	}
	s.Release("a", "other-job") // ignored
	if _, err := s.Claim("a", "job2"); !errors.Is(err, ErrBusy) {
		t.Fatal("release by a different job must not free the session")
	}
	s.Release("a", "job1")
	if got, _ := s.Get("a"); got.State != pb.RuntimeState_RUNTIME_STATE_READY {
		t.Fatalf("released session should be READY: %v", got.State)
	}
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted session still found")
	}
}
