package cli

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

type RemoteRef struct {
	isRemote   bool
	RemoteName string
	Bucket     string
	Path       string
	Raw        string
}

type CopyOptions struct {
	Src          RemoteRef
	Dst          RemoteRef
	ChunkSize    int
	Checksum     bool
	DeleteSource bool
	Verbose      bool
}

func SimpleCopy() *cobra.Command {
	var opts CopyOptions
	cmd := &cobra.Command{
		Use:     "transfer",
		Short:   "Simple grover udp based copy to and from grover server",
		Long:    "Simple grover udp based copy to and from grover server",
		Aliases: []string{"c", "cp"},
		RunE: func(cmd *cobra.Command, args []string) error {
			src, err := parseLocation(args[0])
			if err != nil {
				return err
			}
			dst, err := parseLocation(args[1])
			if err != nil {
				return err
			}
			opts.Src = src
			opts.Dst = dst
			return nil
		},
	}

	return cmd
}

var remoteRe = regexp.MustCompile(`^([A-Za-z0-9_\-]+):(.*)$`)

func parseSize(s string) (int64, error) {
	// Accept plain bytes or KiB/MiB/GiB (binary) or KB/MB/GB (decimal).
	type unit struct {
		suffixes []string
		mul      int64
	}
	units := []unit{
		{[]string{"KiB", "Kib", "Ki"}, 1024},
		{[]string{"MiB", "Mib", "Mi"}, 1024 * 1024},
		{[]string{"GiB", "Gib", "Gi"}, 1024 * 1024 * 1024},
		{[]string{"KB", "Kb", "K"}, 1000},
		{[]string{"MB", "Mb", "M"}, 1000 * 1000},
		{[]string{"GB", "Gb", "G"}, 1000 * 1000 * 1000},
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	// Try exact int first
	if n, err := parseInt64Strict(s); err == nil {
		return n, nil
	}
	// Try with suffix
	for _, u := range units {
		for _, suf := range u.suffixes {
			if strings.HasSuffix(s, suf) {
				num := strings.TrimSpace(strings.TrimSuffix(s, suf))
				n, err := parseInt64Strict(num)
				if err != nil {
					return 0, err
				}
				return n * u.mul, nil
			}
		}
	}
	return 0, fmt.Errorf("unrecognized size %q", s)
}

func parseInt64Strict(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscan(s, &n)
	return n, err
}

func parseLocation(s string) (RemoteRef, error) {
	out := RemoteRef{Raw: s}
	m := remoteRe.FindStringSubmatch(s)
	if m == nil {
		out.isRemote = false
		out.Path = s
		return out, nil
	}
	out.isRemote = true
	out.RemoteName = m[1]
	rest := m[2]
	if rest == "" {
		return out, fmt.Errorf("invalid remote spec %q", s)
	}
	if strings.HasPrefix(rest, "/") {
		out.Path = rest
		return out, nil
	}
	// split "bucket/path"
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		out.Bucket = rest
		out.Path = ""
	} else {
		out.Bucket = rest[:slash]
		if slash+1 < len(rest) {
			out.Path = rest[slash+1:]
		}
	}
	return out, nil
}
