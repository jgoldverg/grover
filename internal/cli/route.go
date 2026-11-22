package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/jgoldverg/grover/internal/orchestrate"
	"github.com/jgoldverg/grover/internal/pb"
	"github.com/jgoldverg/grover/internal/routes"
)

func routeCommand(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "route",
		Short: "Define routes and prepare/close their sessions",
		Long: `A route is a named network path: source node, optional relays, destination node,
protocol, and which side dials (connect-from). Routes are stored on the default node.

"route prepare" materializes a route into a session: it opens the data endpoints
and relay forwards and records the session on the route's source node. Sessions
stay up until "route close" (or a groverd restart) and can run many jobs, one at
a time.`,
	}
	cmd.AddCommand(routePut(a), routeList(a), routeGet(a), routeDelete(a), routePrepare(a), routeClose(a), routeStatus(a))
	return cmd
}

func routePut(a *app) *cobra.Command {
	var source, destination, protocol, connectFrom string
	var via []string
	cmd := &cobra.Command{
		Use:   "put <name>",
		Short: "Create or replace a route",
		Example: `  grover route put uc-tacc --source uc --destination tacc
  grover route put edu-in --source uc --destination edu --connect-from destination
  grover route put via-relay --source uc --destination tacc --via relay1 --protocol udp`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if source == "" || destination == "" {
				return errorf("--source and --destination are required")
			}
			r := &pb.Route{Name: args[0]}
			var err error
			if r.Protocol, err = routes.ParseProtocol(protocol); err != nil {
				return err
			}
			if r.ConnectFrom, err = routes.ParseOrigin(connectFrom); err != nil {
				return err
			}
			// Store addresses, not profile names, so the route works from any CLI.
			if r.Source, err = a.pool.Addr(source); err != nil {
				return err
			}
			if r.Destination, err = a.pool.Addr(destination); err != nil {
				return err
			}
			for _, hop := range splitList(via) {
				addr, err := a.pool.Addr(hop)
				if err != nil {
					return err
				}
				r.Via = append(r.Via, addr)
			}
			c, err := a.conn(cmd.Context(), "")
			if err != nil {
				return err
			}
			saved, err := c.Routes.PutRoute(cmd.Context(), r)
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), saved)
			}
			printRoutes(cmd.OutOrStdout(), []*pb.Route{saved})
			return nil
		},
	}
	cmd.Flags().StringVar(&source, "source", "", "source node (profile or host:port)")
	cmd.Flags().StringVar(&destination, "destination", "", "destination node (profile or host:port)")
	cmd.Flags().StringSliceVar(&via, "via", nil, "relay nodes in order from source to destination (repeat or comma-separate)")
	cmd.Flags().StringVar(&protocol, "protocol", "tcp", "data protocol: tcp|udp")
	cmd.Flags().StringVar(&connectFrom, "connect-from", "source", "which side dials data connections: source|destination")
	return cmd
}

func routeList(a *app) *cobra.Command {
	return &cobra.Command{
		Use: "list", Aliases: []string{"ls"}, Short: "List routes", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := a.conn(cmd.Context(), "")
			if err != nil {
				return err
			}
			list, err := c.Routes.ListRoutes(cmd.Context(), &pb.Empty{})
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), list)
			}
			printRoutes(cmd.OutOrStdout(), list.GetRoutes())
			return nil
		},
	}
}

func routeGet(a *app) *cobra.Command {
	return &cobra.Command{
		Use: "get <name>", Short: "Show one route", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := a.route(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), r)
			}
			printRoutes(cmd.OutOrStdout(), []*pb.Route{r})
			return nil
		},
	}
}

func routeDelete(a *app) *cobra.Command {
	return &cobra.Command{
		Use: "delete <name>", Short: "Delete a route definition (sessions are not touched)", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := a.conn(cmd.Context(), "")
			if err != nil {
				return err
			}
			if _, err := c.Routes.DeleteRoute(cmd.Context(), &pb.RouteRef{Name: args[0]}); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "deleted route %s\n", args[0])
			return nil
		},
	}
}

func routePrepare(a *app) *cobra.Command {
	var sessionID string
	var connections uint32
	cmd := &cobra.Command{
		Use:   "prepare <route>",
		Short: "Open endpoints and relay forwards for a route and record a session",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := a.route(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			spec := orchestrate.SpecFromRoute(r, sessionID)
			spec.Connections = connections
			s, err := orchestrate.Prepare(cmd.Context(), a.pool, spec)
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), s)
			}
			printSession(cmd.OutOrStdout(), s)
			fmt.Fprintf(cmd.OutOrStdout(), "\nrun jobs with: grover transfer --route %s <source-path>... <destination-dir>\n", r.Name)
			return nil
		},
	}
	cmd.Flags().StringVar(&sessionID, "session-id", "", "session id (default: <route>-<timestamp>)")
	cmd.Flags().Uint32Var(&connections, "connections", 0, "connect-from=destination: connections the destination keeps open (default 16)")
	return cmd
}

func routeClose(a *app) *cobra.Command {
	var sessionID string
	cmd := &cobra.Command{
		Use:   "close <route>",
		Short: "Close a route's idle sessions (or one with --session-id)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := a.route(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			ids := []string{sessionID}
			if sessionID == "" {
				src, err := a.conn(cmd.Context(), r.Source)
				if err != nil {
					return err
				}
				list, err := src.Sessions.ListSessions(cmd.Context(), &pb.ListSessionsRequest{RouteId: r.Name})
				if err != nil {
					return err
				}
				ids = nil
				for _, s := range list.GetSessions() {
					if s.GetActiveJobId() != "" {
						fmt.Fprintf(cmd.OutOrStdout(), "skipping %s: running job %s\n", s.SessionId, s.ActiveJobId)
						continue
					}
					ids = append(ids, s.SessionId)
				}
				if len(ids) == 0 {
					fmt.Fprintln(cmd.OutOrStdout(), "no idle sessions to close")
					return nil
				}
			}
			var errs []error
			for _, id := range ids {
				if err := orchestrate.Close(cmd.Context(), a.pool, r.Source, id); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", id, err))
					continue
				}
				fmt.Fprintf(cmd.OutOrStdout(), "closed session %s\n", id)
			}
			return errors.Join(errs...)
		},
	}
	cmd.Flags().StringVar(&sessionID, "session-id", "", "close only this session")
	return cmd
}

func routeStatus(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "status <route>",
		Short: "Show a route's sessions, per-hop relay stats, and running jobs",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			r, err := a.route(ctx, args[0])
			if err != nil {
				return err
			}
			src, err := a.conn(ctx, r.Source)
			if err != nil {
				return err
			}
			list, err := src.Sessions.ListSessions(ctx, &pb.ListSessionsRequest{RouteId: r.Name})
			if err != nil {
				return err
			}
			if a.jsonOut {
				return printProto(cmd.OutOrStdout(), list)
			}
			w := cmd.OutOrStdout()
			printRoutes(w, []*pb.Route{r})
			if len(list.GetSessions()) == 0 {
				fmt.Fprintf(w, "\nno sessions; run: grover route prepare %s\n", r.Name)
				return nil
			}
			for _, s := range list.GetSessions() {
				fmt.Fprintln(w)
				printSession(w, s)
				for _, hop := range s.GetHops() {
					fwLine := "unreachable"
					if rc, err := a.conn(ctx, hop.Control); err == nil {
						if fw, err := rc.Relays.GetForward(ctx, &pb.ForwardRef{ForwardId: hop.ForwardId}); err == nil {
							st := fw.GetStats()
							fwLine = fmt.Sprintf("%s  in %s  out %s  conns %d  errors %d  avg %s", stateName(fw.State),
								formatBytes(st.GetBytesIn()), formatBytes(st.GetBytesOut()), st.GetActiveConnections(), st.GetErrors(), formatRate(st.GetAverageBps()))
						} else {
							fwLine = err.Error()
						}
					}
					fmt.Fprintf(w, "  hop %d %s: %s\n", hop.Index, hop.Control, fwLine)
				}
				if id := s.GetActiveJobId(); id != "" {
					if j, err := src.Transfers.GetJob(ctx, &pb.GetJobRequest{JobId: id}); err == nil {
						fmt.Fprintf(w, "  job %s: %s\n", id, progressLine(j))
					}
				}
			}
			return nil
		},
	}
}

// route loads a route from the default node.
func (a *app) route(ctx context.Context, name string) (*pb.Route, error) {
	c, err := a.conn(ctx, "")
	if err != nil {
		return nil, err
	}
	r, err := c.Routes.GetRoute(ctx, &pb.RouteRef{Name: name})
	if err != nil {
		return nil, fmt.Errorf("route %q on %s: %w", name, c.Node.Name, err)
	}
	return r, nil
}

func printRoutes(w io.Writer, rs []*pb.Route) {
	t := table(w)
	fmt.Fprintln(t, "ROUTE\tSOURCE\tVIA\tDESTINATION\tPROTOCOL\tCONNECT-FROM")
	for _, r := range rs {
		fmt.Fprintf(t, "%s\t%s\t%s\t%s\t%s\t%s\n", r.Name, r.Source, dash(strings.Join(r.Via, ",")), r.Destination,
			routes.ProtocolName(r.Protocol), routes.OriginName(r.ConnectFrom))
	}
	t.Flush()
}

func printSession(w io.Writer, s *pb.RouteSession) {
	t := table(w)
	fmt.Fprintf(t, "session\t%s\n", s.SessionId)
	fmt.Fprintf(t, "state\t%s\n", stateName(s.State))
	fmt.Fprintf(t, "protocol\t%s, %s dials\n", routes.ProtocolName(s.Protocol), routes.OriginName(s.ConnectFrom))
	fmt.Fprintf(t, "source\t%s\n", s.SourceControl)
	fmt.Fprintf(t, "destination\t%s (data %s)\n", s.DestinationControl, endpointLabel(s.GetDestination().GetData()))
	if s.ConnectFrom == pb.ConnectionOrigin_CONNECTION_ORIGIN_DESTINATION {
		fmt.Fprintf(t, "source listener\t%s\n", endpointLabel(s.GetSource().GetData()))
	} else {
		fmt.Fprintf(t, "source sends to\t%s\n", endpointLabel(s.GetData()))
	}
	if s.ActiveJobId != "" {
		fmt.Fprintf(t, "active job\t%s\n", s.ActiveJobId)
	}
	t.Flush()
	for _, h := range s.GetHops() {
		fmt.Fprintf(w, "  hop %d %s: %s -> %s\n", h.Index, h.Control, endpointLabel(h.Ingress), endpointLabel(h.Egress))
	}
}

func splitList(values []string) []string {
	var out []string
	for _, v := range values {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}
