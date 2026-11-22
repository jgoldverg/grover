package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/jgoldverg/grover/internal/pb"
)

func errorf(format string, args ...any) error { return fmt.Errorf(format, args...) }

// table returns a tabwriter; call Flush when done.
func table(w io.Writer) *tabwriter.Writer { return tabwriter.NewWriter(w, 0, 0, 2, ' ', 0) }

func printProto(w io.Writer, m proto.Message) error {
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: true}.Marshal(m)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(b))
	return err
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func stateName(s pb.RuntimeState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "RUNTIME_STATE_"))
}

func roleName(r pb.EndpointRole) string {
	return strings.ToLower(strings.TrimPrefix(r.String(), "ENDPOINT_ROLE_"))
}

func protoName(p pb.DataProtocol) string {
	switch p {
	case pb.DataProtocol_DATA_PROTOCOL_TCP:
		return "tcp"
	case pb.DataProtocol_DATA_PROTOCOL_UDP:
		return "udp"
	}
	return "local"
}

func endpointLabel(ep *pb.DataEndpoint) string {
	if ep.GetPort() == 0 {
		return "-"
	}
	return fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
}

func dash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// formatRate prints bits per second, the unit networks are measured in.
func formatRate(bytesPerSec float64) string {
	bits := bytesPerSec * 8
	switch {
	case bits >= 1e9:
		return fmt.Sprintf("%.2f Gb/s", bits/1e9)
	case bits >= 1e6:
		return fmt.Sprintf("%.1f Mb/s", bits/1e6)
	case bits >= 1e3:
		return fmt.Sprintf("%.1f kb/s", bits/1e3)
	}
	return fmt.Sprintf("%.0f b/s", bits)
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}

func formatTime(unixNano int64) string {
	if unixNano == 0 {
		return "-"
	}
	return time.Unix(0, unixNano).Local().Format("2006-01-02 15:04:05")
}

func percent(done, total uint64) string {
	if total == 0 {
		return "100%"
	}
	return fmt.Sprintf("%.1f%%", float64(done)*100/float64(total))
}

func jobElapsed(j *pb.Job) time.Duration {
	end := j.GetFinishedAtUnixNano()
	if end == 0 {
		end = j.GetSampledAtUnixNano()
	}
	if end == 0 || j.GetStartedAtUnixNano() == 0 {
		return 0
	}
	return time.Duration(end - j.GetStartedAtUnixNano())
}

// parseSize parses 1048576, 1MiB, 8MB, 512K, 1.5G (binary units).
func parseSize(raw string) (uint64, error) {
	s := strings.ToUpper(strings.TrimSpace(raw))
	if s == "" || s == "0" {
		return 0, nil
	}
	s = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "I")
	mult := float64(1)
	if n := len(s); n > 0 {
		switch s[n-1] {
		case 'K':
			mult, s = 1<<10, s[:n-1]
		case 'M':
			mult, s = 1<<20, s[:n-1]
		case 'G':
			mult, s = 1<<30, s[:n-1]
		case 'T':
			mult, s = 1<<40, s[:n-1]
		}
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("invalid size %q (examples: 1048576, 512KiB, 8MiB, 1G)", raw)
	}
	return uint64(v * mult), nil
}

// printJobSummary prints a multi-line summary of a job.
func printJobSummary(w io.Writer, j *pb.Job) {
	t := table(w)
	fmt.Fprintf(t, "job\t%s\n", j.GetJobId())
	fmt.Fprintf(t, "role\t%s\n", roleName(j.GetRole()))
	fmt.Fprintf(t, "state\t%s\n", stateName(j.GetState()))
	if j.GetRouteId() != "" || j.GetSessionId() != "" {
		fmt.Fprintf(t, "route / session\t%s / %s\n", dash(j.GetRouteId()), dash(j.GetSessionId()))
	}
	fmt.Fprintf(t, "protocol\t%s\n", protoName(j.GetProtocol()))
	fmt.Fprintf(t, "destination\t%s %s\n", j.GetDestination(), dataSuffix(j))
	fmt.Fprintf(t, "files\t%d done, %d failed, %d active of %d\n", j.GetFilesDone(), j.GetFilesFailed(), j.GetFilesActive(), j.GetFilesTotal())
	fmt.Fprintf(t, "bytes\t%s of %s (%s)\n", formatBytes(j.GetBytesDone()), formatBytes(j.GetBytesTotal()), percent(j.GetBytesDone(), j.GetBytesTotal()))
	fmt.Fprintf(t, "throughput\t%s now, %s average\n", formatRate(j.GetCurrentBps()), formatRate(j.GetAverageBps()))
	fmt.Fprintf(t, "elapsed\t%s\n", formatDuration(jobElapsed(j)))
	if j.GetRetransmits() > 0 || j.GetRttMs() > 0 {
		fmt.Fprintf(t, "udp\t%d retransmits, rtt %.2f ms\n", j.GetRetransmits(), j.GetRttMs())
	}
	if tu := j.GetTuning(); tu != nil {
		fmt.Fprintf(t, "tuning\tconcurrency=%d parallelism=%d chunk=%s", tu.GetConcurrency(), tu.GetParallelism(), formatBytes(tu.GetChunkSizeBytes()))
		if j.GetProtocol() == pb.DataProtocol_DATA_PROTOCOL_UDP {
			fmt.Fprintf(t, " mtu=%d window=%d flow=%s", tu.GetUdpMtu(), tu.GetUdpWindowPackets(), tu.GetUdpFlowControl())
		}
		fmt.Fprintln(t)
	}
	if j.GetError() != "" {
		fmt.Fprintf(t, "error\t%s\n", j.GetError())
	}
	t.Flush()
	printFiles(w, j.GetFiles())
}

func dataSuffix(j *pb.Job) string {
	if j.GetDestinationData().GetPort() == 0 {
		return ""
	}
	return "(via " + endpointLabel(j.GetDestinationData()) + ")"
}

func printFiles(w io.Writer, files []*pb.FileState) {
	if len(files) == 0 {
		return
	}
	fmt.Fprintln(w)
	t := table(w)
	fmt.Fprintln(t, "FILE\tSTATE\tPROGRESS\tSTREAMS\tERROR")
	for _, f := range files {
		active := 0
		for _, s := range f.GetStreams() {
			if s.GetState() == pb.RuntimeState_RUNTIME_STATE_RUNNING {
				active++
			}
		}
		fmt.Fprintf(t, "%s\t%s\t%s of %s\t%d/%d\t%s\n", f.GetRelativePath(), stateName(f.GetState()),
			formatBytes(f.GetBytesDone()), formatBytes(f.GetSize()), active, len(f.GetStreams()), dash(f.GetError()))
	}
	t.Flush()
}

// progressLine is the single-line live view of a running job.
func progressLine(j *pb.Job) string {
	return fmt.Sprintf("%s  %s  %s/%s  files %d/%d  %s  streams %d  %s",
		stateName(j.GetState()), percent(j.GetBytesDone(), j.GetBytesTotal()),
		formatBytes(j.GetBytesDone()), formatBytes(j.GetBytesTotal()),
		j.GetFilesDone(), j.GetFilesTotal(), formatRate(j.GetCurrentBps()), j.GetStreamsActive(),
		formatDuration(jobElapsed(j)))
}

// isTTY reports whether w is an interactive terminal.
func isTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
