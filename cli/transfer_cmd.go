package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/groverclient"
	udppb "github.com/jgoldverg/grover/pkg/groverpb/groverudpv1"
	"github.com/spf13/cobra"
)

const (
	defaultConcurrency   = 4
	defaultParallelism   = 4
	defaultPipelining    = 2
	defaultChunkSize     = 4 * 1024 * 1024 // 4 MiB
	defaultRateLimitMbps = 0
	defaultMaxRetries    = 3
	defaultBackoffMs     = 500
	defaultOverwrite     = "if-different"
	defaultChecksum      = "none"
)

type TransferCommandOpts struct {
	SourceCredID   string
	DestCredID     string
	FileEntries    []string
	DestPath       string
	IdempotencyKey string
	TransferParams FileTransferParamsOpts
}

type FileTransferParamsOpts struct {
	Concurrency    uint
	Parallelism    uint
	Pipelining     uint
	ChunkSize      uint64
	RateLimitMbps  uint
	VerifyChecksum bool
	MaxRetries     uint
	RetryBackoffMs uint
	Overwrite      string
	Checksum       string
}

func TransferCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "transfer",
		Short: "Commands related to transferring files",
		Long:  "Commands related to transferring files",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.PersistentFlags().String("via", "", "Where to execute: auto|client|server")
	cmd.PersistentFlags().Lookup("via").NoOptDefVal = "auto" // optional
	cmd.AddCommand(newDownloadCommand(), newUploadCommand())
	return cmd
}

func newDownloadCommand() *cobra.Command {
	opts := defaultTransferOptions()
	cmd := &cobra.Command{
		Use:   "download [source paths...]",
		Short: "Download remote files to a local destination",
		Long:  "Download remote files or directories and place them at the provided destination path on the target host.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtimeOpts := opts
			runtimeOpts.FileEntries = append([]string(nil), args...)
			return launchTransfer(cmd, "download", &runtimeOpts)
		},
	}
	bindTransferFlags(cmd, &opts)
	return cmd
}

func newUploadCommand() *cobra.Command {
	opts := defaultTransferOptions()
	cmd := &cobra.Command{
		Use:   "upload [source paths...]",
		Short: "Upload local files to a remote destination",
		Long:  "Upload local files or directories to the provided destination path on the target host.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runtimeOpts := opts
			runtimeOpts.FileEntries = append([]string(nil), args...)
			return launchTransfer(cmd, "upload", &runtimeOpts)
		},
	}
	bindTransferFlags(cmd, &opts)
	return cmd
}

func bindTransferFlags(cmd *cobra.Command, opts *TransferCommandOpts) {
	cmd.Flags().StringVar(&opts.SourceCredID, "source-credential-id", opts.SourceCredID, "Credential ID for the source backend")
	cmd.Flags().StringVar(&opts.DestCredID, "dest-credential-id", opts.DestCredID, "Credential ID for the destination backend")
	cmd.Flags().StringVar(&opts.DestPath, "destination-path", opts.DestPath, "Destination path where files are written")
	cmd.Flags().UintVar(&opts.TransferParams.Concurrency, "concurrency", opts.TransferParams.Concurrency, "Maximum number of files transferred concurrently")
	cmd.Flags().UintVar(&opts.TransferParams.Parallelism, "parallelism", opts.TransferParams.Parallelism, "Maximum parallel operations per file")
	cmd.Flags().UintVar(&opts.TransferParams.Pipelining, "pipelining", opts.TransferParams.Pipelining, "Number of in-flight chunks per worker")
	cmd.Flags().Uint64Var(&opts.TransferParams.ChunkSize, "chunk-size", opts.TransferParams.ChunkSize, "Chunk size in bytes")
	cmd.Flags().UintVar(&opts.TransferParams.RateLimitMbps, "rate-limit", opts.TransferParams.RateLimitMbps, "Rate limit for transfer in Mbps (0 disables)")
	cmd.Flags().BoolVar(&opts.TransferParams.VerifyChecksum, "verify-checksum", opts.TransferParams.VerifyChecksum, "Verify checksum after transfer completion")
	cmd.Flags().UintVar(&opts.TransferParams.MaxRetries, "max-retries", opts.TransferParams.MaxRetries, "Maximum number of retries per chunk")
	cmd.Flags().UintVar(&opts.TransferParams.RetryBackoffMs, "retry-backoff-ms", opts.TransferParams.RetryBackoffMs, "Backoff between retries in milliseconds")
	cmd.Flags().StringVar(&opts.TransferParams.Overwrite, "overwrite", opts.TransferParams.Overwrite, "Overwrite policy: always|if-newer|never|if-different|unspecified")
	cmd.Flags().StringVar(&opts.TransferParams.Checksum, "checksum", opts.TransferParams.Checksum, "Checksum strategy: none|md5|sha256|xxh3")
}

func launchTransfer(cmd *cobra.Command, direction string, opts *TransferCommandOpts) error {
	if len(opts.FileEntries) == 0 {
		return errors.New("at least one source path must be provided")
	}
	appCfg := GetAppConfig(cmd)
	if appCfg == nil {
		return errors.New("app config not found; did PersistentPreRun execute?")
	}
	policy := resolveRoutePolicy(cmd, appCfg)
	gc := groverclient.NewGroverClient(*appCfg)
	if err := gc.Initialize(cmd.Context(), policy); err != nil {
		return err
	}
	defer gc.Close()

	req, err := buildTransferRequest(opts)
	if err != nil {
		return err
	}

	internal.Info("launching file transfer", internal.Fields{
		"direction":   direction,
		"source_cred": opts.SourceCredID,
		"dest_cred":   opts.DestCredID,
		"dest_path":   opts.DestPath,
		"entry_count": len(opts.FileEntries),
		"idempotency": opts.IdempotencyKey,
	})

	resp, err := gc.ServerClient.LaunchFileTransfer(cmd.Context(), req)
	if err != nil {
		return err
	}

	if !resp.GetAccepted() {
		msg := strings.TrimSpace(resp.GetMessage())
		if msg == "" {
			msg = "transfer was not accepted by the server"
		}
		return fmt.Errorf(msg)
	}

	cmd.Printf("transfer %s accepted with id %s\n", direction, resp.GetTransferId())
	return nil
}

func buildTransferRequest(opts *TransferCommandOpts) (*udppb.FileTransferRequest, error) {
	params, err := opts.TransferParams.toProto()
	if err != nil {
		return nil, err
	}
	entries := make([]*udppb.FsEntry, len(opts.FileEntries))
	for i, path := range opts.FileEntries {
		entries[i] = &udppb.FsEntry{Path: path}
	}
	return &udppb.FileTransferRequest{
		SourceCredId:    opts.SourceCredID,
		DestCredId:      opts.DestCredID,
		FsEntry:         entries,
		DestinationPath: opts.DestPath,
		Params:          params,
		IdempotencyKey:  opts.IdempotencyKey,
	}, nil
}

func (p *FileTransferParamsOpts) toProto() (*udppb.FileTransferParams, error) {
	overwrite, err := parseOverwritePolicy(p.Overwrite)
	if err != nil {
		return nil, err
	}
	checksum, err := parseChecksumType(p.Checksum)
	if err != nil {
		return nil, err
	}
	return &udppb.FileTransferParams{
		Concurrency:    uint32(p.Concurrency),
		Parallelism:    uint32(p.Parallelism),
		Pipelining:     uint32(p.Pipelining),
		ChunkSize:      p.ChunkSize,
		RateLimitMbps:  uint32(p.RateLimitMbps),
		Overwrite:      overwrite,
		ChecksumType:   checksum,
		VerifyChecksum: p.VerifyChecksum,
		MaxRetries:     uint32(p.MaxRetries),
		RetryBackoffMs: uint32(p.RetryBackoffMs),
	}, nil
}

func parseOverwritePolicy(raw string) (udppb.OverwritePolicy, error) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "_", "-")) {
	case "", "unspecified", "default":
		return udppb.OverwritePolicy_OVERWRITE_UNSPECIFIED, nil
	case "always":
		return udppb.OverwritePolicy_OVERWRITE_ALWAYS, nil
	case "if-newer", "newer":
		return udppb.OverwritePolicy_OVERWRITE_IF_NEWER, nil
	case "never":
		return udppb.OverwritePolicy_OVERWRITE_NEVER, nil
	case "if-different", "different":
		return udppb.OverwritePolicy_OVERWRITE_IF_DIFFERENT, nil
	default:
		return 0, fmt.Errorf("unknown overwrite policy %q", raw)
	}
}

func parseChecksumType(raw string) (udppb.ChecksumType, error) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "_", "")) {
	case "", "none":
		return udppb.ChecksumType_CHECKSUM_NONE, nil
	case "md5":
		return udppb.ChecksumType_CHECKSUM_MD5, nil
	case "sha256", "sha-256":
		return udppb.ChecksumType_CHECKSUM_SHA256, nil
	case "xxh3", "xxhash3":
		return udppb.ChecksumType_CHECKSUM_XXH3, nil
	default:
		return 0, fmt.Errorf("unknown checksum type %q", raw)
	}
}

func resolveRoutePolicy(cmd *cobra.Command, cfg *internal.AppConfig) groverclient.RoutePolicy {
	route := cfg.Route
	if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
		if v, err := cmd.Flags().GetString("via"); err == nil && v != "" {
			route = v
		}
	}
	return groverclient.ParseRoutePolicy(route)
}

func defaultTransferOptions() TransferCommandOpts {
	return TransferCommandOpts{
		TransferParams: FileTransferParamsOpts{
			Concurrency:    defaultConcurrency,
			Parallelism:    defaultParallelism,
			Pipelining:     defaultPipelining,
			ChunkSize:      defaultChunkSize,
			RateLimitMbps:  defaultRateLimitMbps,
			VerifyChecksum: false,
			MaxRetries:     defaultMaxRetries,
			RetryBackoffMs: defaultBackoffMs,
			Overwrite:      defaultOverwrite,
			Checksum:       defaultChecksum,
		},
	}
}
