package cli

import (
	"fmt"
	"io"
	"strings"
)

type TransferRoutePlan struct {
	Source           RemoteRef
	Destination      RemoteRef
	Relays           []TransferRelayHop
	Hops             []TransferRouteHop
	Protocol         string
	ParallelStreams  int
	Concurrency      int
	Mode             string
	Direction        string
	ConnectionOrigin string
	DataDirection    string
}

type TransferRelayHop struct {
	Raw             string
	ControlEndpoint string
	DataEndpoint    string
}

type TransferRouteHop struct {
	Index           int
	Role            string
	Label           string
	ControlEndpoint string
	DataEndpoint    string
}

func buildTransferRoutePlan(src RemoteRef, dst RemoteRef, opts CopyOptions) (TransferRoutePlan, error) {
	relays, err := parseTransferRelays(opts.Via)
	if err != nil {
		return TransferRoutePlan{}, err
	}
	protocol := strings.ToLower(strings.TrimSpace(opts.Protocol))
	if protocol == "" {
		protocol = "config"
	}
	parallelStreams := opts.ParallelStreams
	if parallelStreams <= 0 {
		parallelStreams = 1
	}
	concurrency := opts.effectiveConcurrency()
	mode := "direct"
	if len(relays) > 0 {
		mode = "bridge"
	}
	direction := transferDirection(src, dst)
	connectionOrigin, err := normalizeConnectionOrigin(opts.ConnectionOrigin)
	if err != nil {
		return TransferRoutePlan{}, err
	}
	dataDirection, err := normalizeDataDirection(opts.DataDirection)
	if err != nil {
		return TransferRoutePlan{}, err
	}
	hops := buildTransferRouteHops(src, dst, relays)
	return TransferRoutePlan{
		Source:           src,
		Destination:      dst,
		Relays:           relays,
		Hops:             hops,
		Protocol:         protocol,
		ParallelStreams:  parallelStreams,
		Concurrency:      concurrency,
		Mode:             mode,
		Direction:        direction,
		ConnectionOrigin: connectionOrigin,
		DataDirection:    dataDirection,
	}, nil
}

func parseTransferRelays(values []string) ([]TransferRelayHop, error) {
	relays := make([]TransferRelayHop, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			raw := strings.TrimSpace(part)
			if raw == "" {
				continue
			}
			if strings.Contains(raw, "/") {
				return nil, fmt.Errorf("invalid relay hop %q: relay hops must be names or host:port values, not paths", raw)
			}
			relays = append(relays, TransferRelayHop{
				Raw:             raw,
				ControlEndpoint: raw,
				DataEndpoint:    "allocated-by-relay",
			})
		}
	}
	return relays, nil
}

func buildTransferRouteHops(src RemoteRef, dst RemoteRef, relays []TransferRelayHop) []TransferRouteHop {
	hops := make([]TransferRouteHop, 0, len(relays)+2)
	hops = append(hops, TransferRouteHop{
		Index:           0,
		Role:            "source",
		Label:           endpointLabel(src, "local"),
		ControlEndpoint: controlEndpointLabel(src, "local"),
		DataEndpoint:    "source",
	})
	for _, relay := range relays {
		hops = append(hops, TransferRouteHop{
			Index:           len(hops),
			Role:            "relay",
			Label:           relay.Raw,
			ControlEndpoint: relay.ControlEndpoint,
			DataEndpoint:    relay.DataEndpoint,
		})
	}
	hops = append(hops, TransferRouteHop{
		Index:           len(hops),
		Role:            "destination",
		Label:           endpointLabel(dst, "local"),
		ControlEndpoint: controlEndpointLabel(dst, "local"),
		DataEndpoint:    "allocated-by-destination",
	})
	return hops
}

func printTransferRoutePlan(w io.Writer, plan TransferRoutePlan) {
	if w == nil {
		return
	}
	labels := make([]string, 0, len(plan.Hops))
	for _, hop := range plan.Hops {
		labels = append(labels, hop.Label)
	}
	fmt.Fprintf(w, "route: %s\n", strings.Join(labels, " -> "))
	fmt.Fprintf(w, "mode: %s\n", plan.Mode)
	fmt.Fprintf(w, "direction: %s\n", plan.Direction)
	fmt.Fprintf(w, "connection_origin: %s\n", plan.ConnectionOrigin)
	fmt.Fprintf(w, "data_direction: %s\n", plan.DataDirection)
	fmt.Fprintf(w, "protocol: %s\n", plan.Protocol)
	fmt.Fprintf(w, "parallel_streams: %d\n", plan.ParallelStreams)
	fmt.Fprintf(w, "concurrency: %d\n", plan.Concurrency)
	for _, hop := range plan.Hops {
		fmt.Fprintf(w, "hop[%d]: role=%s endpoint=%s control_endpoint=%s data_endpoint=%s\n", hop.Index, hop.Role, hop.Label, hop.ControlEndpoint, hop.DataEndpoint)
	}
}

func normalizeConnectionOrigin(origin string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(origin))
	normalized = strings.ReplaceAll(normalized, "_", "-")
	if normalized == "" {
		return "source", nil
	}
	switch normalized {
	case "source", "src":
		return "source", nil
	case "destination", "dest", "dst":
		return "destination", nil
	default:
		return "", fmt.Errorf("invalid --connect-from/--connection-origin %q: must be source or destination", origin)
	}
}

func normalizeDataDirection(direction string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(direction))
	normalized = strings.ReplaceAll(normalized, "_", "-")
	if normalized == "" {
		return "source-to-destination", nil
	}
	switch normalized {
	case "source-to-destination", "source-destination", "src-to-dst", "forward", "fwd":
		return "source-to-destination", nil
	case "destination-to-source", "destination-source", "dst-to-src", "reverse", "rev":
		return "destination-to-source", nil
	default:
		return "", fmt.Errorf("invalid --flow/--data-direction %q: must be forward/source-to-destination or reverse/destination-to-source", direction)
	}
}

func normalizeTransferDirection(direction string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(direction))
	normalized = strings.ReplaceAll(normalized, "_", "-")
	if normalized == "" {
		return "forward", nil
	}
	switch normalized {
	case "forward", "fwd":
		return "forward", nil
	case "reverse", "rev":
		return "reverse", nil
	default:
		return "", fmt.Errorf("invalid --direction %q: must be forward or reverse", direction)
	}
}

func transferDirection(src RemoteRef, dst RemoteRef) string {
	switch {
	case !src.isRemote && dst.isRemote:
		return "upload"
	case src.isRemote && !dst.isRemote:
		return "download"
	case src.isRemote && dst.isRemote:
		return "remote-to-remote"
	default:
		return "local"
	}
}

func endpointLabel(ref RemoteRef, localLabel string) string {
	if ref.isRemote {
		if strings.TrimSpace(ref.ControlEndpoint) != "" {
			return ref.ControlEndpoint
		}
		if strings.TrimSpace(ref.RemoteName) != "" {
			return ref.RemoteName
		}
		return ref.Raw
	}
	return localLabel
}

func controlEndpointLabel(ref RemoteRef, localLabel string) string {
	if ref.isRemote {
		if strings.TrimSpace(ref.ControlEndpoint) != "" {
			return ref.ControlEndpoint
		}
		return endpointLabel(ref, localLabel)
	}
	return localLabel
}
