package cli

import (
	"strings"

	"github.com/jgoldverg/grover/pkg/util"
	"github.com/spf13/cobra"
)

func resolveRoutePolicy(cmd *cobra.Command, defaultRoute string) util.RoutePolicy {
	route := defaultRoute
	if flag := cmd.Flag("via"); flag != nil {
		if v := strings.TrimSpace(flag.Value.String()); v != "" {
			route = v
		}
	}
	return util.ParseRoutePolicy(route)
}
