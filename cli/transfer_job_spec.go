package cli

import (
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"
)

type transferJobSpec struct {
	Source      string              `toml:"source"`
	Destination string              `toml:"destination"`
	Transfer    transferSpecOptions `toml:"transfer"`
	Route       transferSpecRoute   `toml:"route"`
}

type transferSpecOptions struct {
	Protocol        string `toml:"protocol"`
	ParallelStreams int    `toml:"parallel_streams"`
	Concurrency     int    `toml:"concurrency"`
}

type transferSpecRoute struct {
	Via []string `toml:"via"`
}

func applyTransferJobSpec(cmd *cobra.Command, routeFile string, args []string, opts *CopyOptions) ([]string, error) {
	routeFile = strings.TrimSpace(routeFile)
	if routeFile == "" {
		return args, nil
	}
	var spec transferJobSpec
	if _, err := toml.DecodeFile(routeFile, &spec); err != nil {
		return nil, fmt.Errorf("load route file %s: %w", routeFile, err)
	}
	if len(args) == 0 {
		src := strings.TrimSpace(spec.Source)
		dst := strings.TrimSpace(spec.Destination)
		if src == "" || dst == "" {
			return nil, fmt.Errorf("route file %s must set source and destination when command args are omitted", routeFile)
		}
		args = []string{src, dst}
	}
	if opts == nil {
		return args, nil
	}
	if !cmd.Flags().Changed("protocol") && strings.TrimSpace(spec.Transfer.Protocol) != "" {
		opts.Protocol = spec.Transfer.Protocol
	}
	if !cmd.Flags().Changed("parallel-streams") && spec.Transfer.ParallelStreams > 0 {
		opts.ParallelStreams = spec.Transfer.ParallelStreams
	}
	if !cmd.Flags().Changed("concurrency") && spec.Transfer.Concurrency > 0 {
		opts.Concurrency = spec.Transfer.Concurrency
	}
	if !cmd.Flags().Changed("via") && len(spec.Route.Via) > 0 {
		opts.Via = append([]string(nil), spec.Route.Via...)
	}
	return args, nil
}
