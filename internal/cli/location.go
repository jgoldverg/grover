package cli

import (
	"fmt"
	"path"
	"strings"

	"github.com/jgoldverg/grover/internal/transfer"
)

// location is a transfer argument: a path on a node.
type location struct {
	Node string // profile name or host:port; empty means the default node
	Path string // absolute path, or synthetic://... for sources
}

// parseLocation accepts:
//
//	/abs/path                 path on the default node
//	uc:/abs/path              path on profile "uc"
//	host:22444:/abs/path      path on a node by address
//	[::1]:22444:/abs/path     IPv6 address
//	synthetic://<bytes>/name  generated zeros (sources only)
//	uc:synthetic://<bytes>/n  generated zeros on node uc
func parseLocation(raw string) (location, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return location{}, fmt.Errorf("empty location")
	}
	if transfer.IsSynthetic(s) || strings.HasPrefix(s, "/") {
		return location{Path: s}, nil
	}
	if i := strings.Index(s, ":synthetic://"); i > 0 {
		return location{Node: s[:i], Path: s[i+1:]}, nil
	}
	i := strings.Index(s, ":/")
	if i <= 0 {
		return location{}, fmt.Errorf("location %q must be /abs/path or node:/abs/path", raw)
	}
	loc := location{Node: s[:i], Path: s[i+1:]}
	if strings.HasPrefix(loc.Node, "[") && !strings.Contains(loc.Node, "]") {
		return location{}, fmt.Errorf("bad IPv6 node in %q; use [addr]:port:/path", raw)
	}
	loc.Path = path.Clean(loc.Path)
	return loc, nil
}

func (l location) String() string {
	if l.Node == "" {
		return l.Path
	}
	return l.Node + ":" + l.Path
}
