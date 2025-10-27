package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	"github.com/jgoldverg/grover/pkg/util"
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
	defaultBatchSize     = 10
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
	BatchSize      uint
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
	cmd.AddCommand(newCopyCommand())
	return cmd
}

type CopyCommandOpts struct {
	TransferCommandOpts
	FromSpecs []string
	ToSpecs   []string
	MapSpecs  []string
	PlanFile  string
}

func defaultCopyOptions() CopyCommandOpts {
	return CopyCommandOpts{TransferCommandOpts: defaultTransferOptions()}
}

func newCopyCommand() *cobra.Command {
	opts := defaultCopyOptions()
	cmd := &cobra.Command{
		Use:   "cp",
		Short: "Copy files between endpoints",
		Long:  "Define source and destination endpoints (via --from/--to) and optionally mapping rules to orchestrate scatter/gather transfers.",
		RunE: func(cmd *cobra.Command, args []string) error {
			runtimeOpts := opts
			return runCopyCommand(cmd, &runtimeOpts)
		},
	}
	bindTransferFlags(cmd, &opts.TransferCommandOpts)
	cmd.Flags().StringArrayVar(&opts.FromSpecs, "from", opts.FromSpecs, "Source endpoint(s). Format: [label=]URI")
	cmd.Flags().StringArrayVar(&opts.ToSpecs, "to", opts.ToSpecs, "Destination endpoint(s). Format: [label=]URI")
	cmd.Flags().StringArrayVar(&opts.MapSpecs, "map", opts.MapSpecs, "Mapping rule in positional form: 'source dest [option=value ...]' (quote if spaces present)")
	cmd.Flags().StringVar(&opts.PlanFile, "plan", opts.PlanFile, "Optional plan file (YAML/JSON) describing complex mappings")
	return cmd
}

func runCopyCommand(cmd *cobra.Command, opts *CopyCommandOpts) error {
	fromInputs, err := inputsFromRawSpecs(opts.FromSpecs)
	if err != nil {
		return err
	}
	toInputs, err := inputsFromRawSpecs(opts.ToSpecs)
	if err != nil {
		return err
	}
	mapSpecs := append([]string(nil), opts.MapSpecs...)

	if opts.PlanFile != "" {
		doc, err := loadTransferPlanDocument(opts.PlanFile)
		if err != nil {
			return err
		}
		planFrom, planTo, planMaps, err := doc.toInputs()
		if err != nil {
			return err
		}
		fromInputs = append(planFrom, fromInputs...)
		toInputs = append(planTo, toInputs...)
		mapSpecs = append(planMaps, mapSpecs...)
		if doc.Params != nil {
			applyPlanParams(&opts.TransferParams, doc.Params)
		}
		if doc.IdempotencyKey != "" {
			opts.IdempotencyKey = doc.IdempotencyKey
		}
		if doc.Via != "" {
			_ = cmd.Flags().Set("via", doc.Via)
		}
	}

	if len(fromInputs) == 0 {
		return errors.New("at least one --from endpoint must be provided")
	}
	if len(toInputs) == 0 {
		return errors.New("at least one --to endpoint must be provided")
	}

	plan, err := newTransferPlan(&opts.TransferCommandOpts, fromInputs, toInputs, mapSpecs)
	if err != nil {
		return err
	}
	printTransferPlan(cmd, plan)
	return launchTransferRequest(cmd, "copy", &opts.TransferCommandOpts, plan.Request)
}

func printTransferPlan(cmd *cobra.Command, plan *transferPlan) {
	cmd.Println("transfer plan:")
	if plan.usedDefaultEdges() {
		cmd.Println("  (no explicit maps provided; defaulting to all sources -> all destinations)")
	}
	for _, line := range plan.summaryLines() {
		cmd.Printf("  %s\n", line)
	}
	cmd.Printf("  total edges: %d\n", len(plan.Request.Edges))
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
	cmd.Flags().UintVar(&opts.TransferParams.BatchSize, "batch-size", opts.TransferParams.BatchSize, "Batch size is the number of read operations to 1 write operation")
}

func launchTransferRequest(cmd *cobra.Command, direction string, opts *TransferCommandOpts, req *backend.TransferRequest) error {
	if len(req.Edges) == 0 {
		return errors.New("transfer request must contain at least one edge")
	}
	appCfg := GetAppConfig(cmd)
	if appCfg == nil {
		return errors.New("app config not found; did PersistentPreRun execute?")
	}
	policy := resolveRoutePolicy(cmd, appCfg)
	gc := gclient.NewClient(*appCfg)
	if err := gc.Initialize(cmd.Context(), policy); err != nil {
		return err
	}
	defer gc.Close()

	internal.Info("launching file transfer", internal.Fields{
		"direction":   direction,
		"source_cred": opts.SourceCredID,
		"dest_cred":   opts.DestCredID,
		"edge_count":  edgeOrEntryCount(req),
		"idempotency": opts.IdempotencyKey,
	})

	transferAPI := gc.Transfer()
	if transferAPI == nil {
		return errors.New("transfer client not initialized")
	}
	resp, err := transferAPI.LaunchTransfer(cmd.Context(), req)
	if err != nil {
		return err
	}

	if !resp.GetAccepted() {
		msg := strings.TrimSpace(resp.GetMessage())
		if msg == "" {
			msg = "transfer was not accepted by the server"
		}
		return errors.New(msg)
	}

	cmd.Printf("transfer %s accepted with id %s\n", direction, resp.GetTransferId())
	return nil
}

func edgeOrEntryCount(req *backend.TransferRequest) int {
	return len(req.Edges)
}

func (p *FileTransferParamsOpts) toBackend() (backend.TransferParams, error) {
	overwrite, err := parseOverwritePolicy(p.Overwrite)
	if err != nil {
		return backend.TransferParams{}, err
	}
	checksum, err := parseChecksumType(p.Checksum)
	if err != nil {
		return backend.TransferParams{}, err
	}
	return backend.TransferParams{
		Concurrency:    uint32(p.Concurrency),
		Parallelism:    uint32(p.Parallelism),
		Pipelining:     uint32(p.Pipelining),
		ChunkSize:      p.ChunkSize,
		RateLimitMbps:  uint32(p.RateLimitMbps),
		Overwrite:      overwrite,
		Checksum:       checksum,
		VerifyChecksum: p.VerifyChecksum,
		MaxRetries:     uint32(p.MaxRetries),
		RetryBackoffMs: uint32(p.RetryBackoffMs),
		BatchSize:      uint32(p.BatchSize),
	}, nil
}

func parseOverwritePolicy(raw string) (backend.OverwritePolicy, error) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "_", "-")) {
	case "", "unspecified", "default":
		return backend.UNSPECIFIED, nil
	case "always":
		return backend.ALWAYS, nil
	case "if-newer", "newer":
		return backend.IF_NEWER, nil
	case "never":
		return backend.NEVER, nil
	case "if-different", "different":
		return backend.IF_DIFFERENT, nil
	default:
		return 0, fmt.Errorf("unknown overwrite policy %q", raw)
	}
}

func parseChecksumType(raw string) (backend.CheckSumType, error) {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), "_", "")) {
	case "", "none":
		return backend.NONE, nil
	case "md5":
		return backend.MD5, nil
	case "sha256", "sha-256":
		return backend.SHA256SUM, nil
	case "xxh3", "xxhash3":
		return backend.XXH3, nil
	default:
		return 0, fmt.Errorf("unknown checksum type %q", raw)
	}
}

func resolveRoutePolicy(cmd *cobra.Command, cfg *internal.AppConfig) util.RoutePolicy {
	route := cfg.Route
	if f := cmd.Flags().Lookup("via"); f != nil && f.Changed {
		if v, err := cmd.Flags().GetString("via"); err == nil && v != "" {
			route = v
		}
	}
	return util.ParseRoutePolicy(route)
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
			BatchSize:      defaultBatchSize,
		},
	}
}
