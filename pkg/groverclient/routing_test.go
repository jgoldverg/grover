package groverclient

import "testing"

func TestParseRoutePolicy(t *testing.T) {
	tests := []struct {
		in       string
		expected RoutePolicy
	}{
		{"", RouteAuto},
		{"  auto  ", RouteAuto},
		{"client", RouteForceLocal},
		{"server", RouteForceRemote},
		{"SERVER", RouteForceRemote},
	}

	for _, tc := range tests {
		if got := ParseRoutePolicy(tc.in); got != tc.expected {
			t.Fatalf("ParseRoutePolicy(%q)=%v want %v", tc.in, got, tc.expected)
		}
	}
}

func TestShouldUseRemote(t *testing.T) {
	if ShouldUseRemote(RouteForceLocal, true) {
		t.Fatalf("force local should not use remote")
	}
	if ShouldUseRemote(RouteForceRemote, false) {
		t.Fatalf("force remote without remote should be false")
	}
	if !ShouldUseRemote(RouteForceRemote, true) {
		t.Fatalf("force remote with remote should be true")
	}
	if !ShouldUseRemote(RouteAuto, true) {
		t.Fatalf("auto with remote available should be true")
	}
	if ShouldUseRemote(RouteAuto, false) {
		t.Fatalf("auto without remote should be false")
	}
}
