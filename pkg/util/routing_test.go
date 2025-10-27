package util

import "testing"

func TestParseRoutePolicy(t *testing.T) {
	tests := []struct {
		in       string
		expected RoutePolicy
	}{
		{"", RouteAuto},
		{"auto", RouteAuto},
		{"server", RouteForceRemote},
		{"client", RouteForceLocal},
		{"  SERVER   ", RouteForceRemote},
		{"CLIENT", RouteForceLocal},
		{"   ", RouteAuto},
	}
	for _, tc := range tests {
		if got := ParseRoutePolicy(tc.in); got != tc.expected {
			t.Fatalf("ParseRoutePolicy(%q)=%v want %v", tc.in, got, tc.expected)
		}
	}
}
