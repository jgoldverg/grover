package groverclient

import (
	"strings"
)

type RoutePolicy int

const (
	RouteAuto RoutePolicy = iota
	RouteForceLocal
	RouteForceRemote
)

func ParseRoutePolicy(s string) RoutePolicy {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "client":
		return RouteForceLocal
	case "server":
		return RouteForceRemote
	default:
		return RouteAuto
	}
}

func ShouldUseRemote(policy RoutePolicy, hasRemote bool) bool {
	switch policy {
	case RouteForceLocal:
		return false
	case RouteForceRemote:
		return hasRemote
	default:
		return hasRemote
	}
}
