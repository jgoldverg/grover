package cli

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/cli/output"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
	"github.com/jgoldverg/grover/pkg/metrics"
	"github.com/jgoldverg/grover/pkg/util"
	"github.com/spf13/cobra"
)

type RemoteRef struct {
	isRemote        bool
	RemoteName      string
	Bucket          string
	Path            string
	Raw             string
	ExpectDirectory bool
}

type CopyOptions struct {
	DeleteSource       bool
	Concurrency        int
	NoUI               bool
	Protocol           string
	UIMode             string
	UIIntervalMs       int
	ParallelStreams    int
	UDPFlowControl     string
	UDPWindowPackets   int
	UDPWindowBytes     int
	UDPAckEveryPackets int
	UDPAckEveryMs      int
	MTU                string
}

func SimpleCopy() *cobra.Command {
	var opts CopyOptions
	cmd := &cobra.Command{
		Use:          "transfer <source> <destination>",
		Short:        "Simple grover udp based copy to and from grover server",
		Long:         "Simple grover udp based copy to and from grover server",
		Aliases:      []string{"c", "cp"},
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.validate(); err != nil {
				return err
			}
			printer := output.NewPrinter()
			src, err := parseLocation(args[0])
			if err != nil {
				return err
			}
			dst, err := parseLocation(args[1])
			if err != nil {
				return err
			}
			switch {
			case src.isRemote && dst.isRemote:
				return fmt.Errorf("remote to remote transfers are not supported yet")
			case !src.isRemote && !dst.isRemote:
				return fmt.Errorf("at least one side must be remote")
			case src.isRemote:
				if opts.DeleteSource {
					return fmt.Errorf("--delete-source is only supported for local sources")
				}
				return downloadFromRemote(cmd, src, dst, opts, printer)
			default:
				return uploadToRemote(cmd, src, dst, opts, printer)
			}
		},
	}
	cmd.Flags().BoolVar(&opts.DeleteSource, "delete-source", false, "Delete the local source file after a successful upload")
	cmd.Flags().IntVar(&opts.Concurrency, "concurrency", 4, "Maximum number of files to transfer in parallel")
	cmd.Flags().BoolVar(&opts.NoUI, "no-ui", false, "Disable live progress and metrics output")
	cmd.Flags().StringVar(&opts.Protocol, "protocol", "", "Transfer data-plane protocol (udp|tcp)")
	cmd.Flags().IntVar(&opts.ParallelStreams, "parallel-streams", 0, "Per-file parallel streams/ranges (0 uses config)")
	cmd.Flags().StringVar(&opts.UDPFlowControl, "udp-flow-control", "", "UDP flow control mode (fixed|bbr)")
	cmd.Flags().IntVar(&opts.UDPWindowPackets, "udp-window-packets", 0, "UDP max in-flight packets per stream (0 uses config)")
	cmd.Flags().IntVar(&opts.UDPWindowBytes, "udp-window-bytes", 0, "UDP max in-flight bytes per stream (0 derives from packets)")
	cmd.Flags().IntVar(&opts.UDPAckEveryPackets, "udp-ack-every-packets", 0, "UDP ACK every N packets (0 uses config)")
	cmd.Flags().IntVar(&opts.UDPAckEveryMs, "udp-ack-every-ms", 0, "UDP ACK interval in milliseconds (0 uses config)")
	cmd.Flags().StringVar(&opts.MTU, "mtu", "", "UDP MTU override (auto|bytes)")
	cmd.Flags().StringVar(&opts.UIMode, "ui", "summary", "Transfer UI mode (summary|live|none)")
	cmd.Flags().IntVar(&opts.UIIntervalMs, "ui-interval-ms", 2000, "Live metrics UI refresh interval in milliseconds")
	return cmd
}

var remoteRe = regexp.MustCompile(`^([A-Za-z0-9_\-]+):(.*)$`)

func parseLocation(input string) (RemoteRef, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return RemoteRef{}, fmt.Errorf("location is required")
	}
	ref := RemoteRef{Raw: trimmed}

	if m := remoteRe.FindStringSubmatch(trimmed); m != nil {
		ref.isRemote = true
		ref.RemoteName = m[1]
		remainder := m[2]
		if remainder == "" {
			return ref, fmt.Errorf("invalid remote spec %q", input)
		}
		ref.ExpectDirectory = remainder == "/" || strings.HasSuffix(remainder, "/")
		if ref.ExpectDirectory && remainder != "/" {
			remainder = strings.TrimSuffix(remainder, "/")
		}
		if strings.HasPrefix(remainder, "/") {
			if remainder == "" {
				remainder = "/"
			}
			ref.Path = remainder
			return ref, nil
		}
		slash := strings.IndexByte(remainder, '/')
		if slash < 0 {
			ref.Bucket = remainder
			return ref, nil
		}
		ref.Bucket = remainder[:slash]
		if slash+1 < len(remainder) {
			ref.Path = remainder[slash+1:]
		} else {
			ref.ExpectDirectory = true
		}
		return ref, nil
	}

	ref.Path = trimmed
	if trimmed != "/" && strings.HasSuffix(trimmed, "/") {
		ref.ExpectDirectory = true
		ref.Path = strings.TrimSuffix(trimmed, "/")
		if ref.Path == "" {
			ref.Path = "/"
		}
	}
	return ref, nil
}

func downloadFromRemote(cmd *cobra.Command, src RemoteRef, dst RemoteRef, opts CopyOptions, printer *output.Printer) error {
	if dst.isRemote {
		return fmt.Errorf("destination must be local when downloading")
	}

	remoteRoot := remotePathString(src)
	if strings.TrimSpace(remoteRoot) == "" {
		return fmt.Errorf("remote source %q is missing a path", src.Raw)
	}

	prevLevel := internal.CurrentLogLevel()
	internal.SetLogLevel(internal.LevelWarn)
	defer internal.SetLogLevel(prevLevel)

	internal.Debug("starting download", internal.Fields{
		"source":      src.Raw,
		"destination": dst.Raw,
	})

	client, err := newTransferClientForRemote(cmd, src, opts)
	if err != nil {
		return err
	}
	defer client.Close()

	transfer := client.Transfer()
	if transfer == nil {
		return fmt.Errorf("transfer service unavailable on remote server")
	}

	collector := metrics.NewTransferCollector("grover")
	if aware, ok := transfer.(*gclient.GroverTransferClient); ok {
		aware.SetMetricsCollector(collector)
	}

	files, err := transfer.Enumerate(cmd.Context(), remoteRoot, true)
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "found %d remote files under %s\n", len(files), remoteRoot)
	if len(files) == 0 {
		return fmt.Errorf("no files found at remote path %q", remoteRoot)
	}

	localBase, treatAsDir, err := resolveDownloadDestination(dst, len(files) > 1 || src.ExpectDirectory)
	if err != nil {
		return err
	}

	jobs := make([]downloadJob, 0, len(files))
	for _, rf := range files {
		rel := strings.TrimSpace(rf.RelativePath)
		if rel == "" {
			rel = path.Base(rf.FullPath)
		}
		localTarget := localBase
		if treatAsDir {
			localTarget = filepath.Join(localBase, filepath.FromSlash(rel))
		}
		label := rel
		if strings.TrimSpace(label) == "" {
			label = filepath.Base(localTarget)
		}
		if strings.TrimSpace(label) == "" {
			label = filepath.Base(rf.FullPath)
		}
		jobs = append(jobs, downloadJob{
			remotePath: rf.FullPath,
			localPath:  localTarget,
			label:      label,
			size:       rf.Size,
		})
	}

	var (
		progress *output.FileProgressManager
		display  *output.MetricsDisplay
		summary  *output.TransferSummary
		done     atomic.Uint64
	)
	stopDisplay := func() {}
	stopProgress := func() {}
	if opts.uiMode() == "live" {
		if len(jobs) > 0 {
			fp := output.NewFileProgressManager("Downloads")
			if err := fp.Start(); err != nil {
				internal.Debug("unable to start download progress", internal.Fields{
					internal.FieldError: err.Error(),
				})
			} else {
				progress = fp
				stopProgress = func() { progress.Stop() }
			}
		}

		display = output.NewMetricsDisplay("Network Telemetry", collector).WithInterval(opts.uiInterval())
		if progress != nil {
			if writer := progress.NewSection(); writer != nil {
				display = display.WithWriter(writer)
			}
		}
		if err := display.Start(cmd.Context()); err != nil {
			internal.Debug("unable to start metrics dashboard", internal.Fields{
				internal.FieldError: err.Error(),
			})
		} else {
			stopDisplay = func() { display.Stop() }
		}
	} else if opts.uiMode() == "summary" {
		summary = output.NewTransferSummary("download", collector, len(jobs), totalDownloadBytes(jobs), &done).
			WithInterval(opts.uiInterval()).
			WithWriter(cmd.OutOrStdout())
		if err := summary.Start(cmd.Context()); err != nil {
			internal.Debug("unable to start transfer summary", internal.Fields{
				internal.FieldError: err.Error(),
			})
		} else {
			stopDisplay = func() { summary.Stop() }
		}
	}
	cleanup := func() {
		stopDisplay()
		stopProgress()
	}
	defer cleanup()

	jobFns := make([]jobFunc, 0, len(jobs))
	for _, job := range jobs {
		job := job
		jobFns = append(jobFns, func(ctx context.Context) error {
			internal.Debug("downloading file", internal.Fields{
				"remote": job.remotePath,
				"local":  job.localPath,
			})
			if err := ensureParentDir(job.localPath); err != nil {
				return err
			}
			out, err := os.Create(job.localPath)
			if err != nil {
				return fmt.Errorf("create %s: %w", job.localPath, err)
			}
			defer out.Close()

			writer := io.Writer(out)
			writer = wrapWriterWithDiskMetrics(writer, collector)
			if progress != nil {
				writer = progress.WrapWriter(job.label, job.size, writer)
			}
			if err := transfer.Get(ctx, job.remotePath, writer); err != nil {
				return fmt.Errorf("download %s -> %s: %w", job.remotePath, job.localPath, err)
			}
			done.Add(1)
			internal.Debug("download complete", internal.Fields{
				"remote": job.remotePath,
				"local":  job.localPath,
			})
			return nil
		})
	}

	err = runJobs(cmd.Context(), opts.effectiveConcurrency(), jobFns)
	cleanup()
	cleanup = func() {}
	return err
}

func uploadToRemote(cmd *cobra.Command, src RemoteRef, dst RemoteRef, opts CopyOptions, printer *output.Printer) error {
	if !dst.isRemote {
		return fmt.Errorf("destination must be remote when uploading")
	}
	if src.isRemote {
		return fmt.Errorf("source must be local when uploading")
	}

	prevLevel := internal.CurrentLogLevel()
	internal.SetLogLevel(internal.LevelWarn)
	defer internal.SetLogLevel(prevLevel)

	internal.Debug("starting upload", internal.Fields{
		"source":      src.Raw,
		"destination": dst.Raw,
	})
	localPath, err := expandUserPath(src.Path)
	if err != nil {
		return err
	}
	info, err := os.Stat(localPath)
	if err != nil {
		return err
	}

	remotePath := remotePathString(dst)
	if info.IsDir() && !dst.ExpectDirectory && strings.TrimSpace(remotePath) != "" {
		return fmt.Errorf("destination %q must end with / when uploading a directory", dst.Raw)
	}

	client, err := newTransferClientForRemote(cmd, dst, opts)
	if err != nil {
		return err
	}
	defer client.Close()

	transfer := client.Transfer()
	if transfer == nil {
		return fmt.Errorf("transfer service unavailable on remote server")
	}

	collector := metrics.NewTransferCollector("grover")
	if aware, ok := transfer.(*gclient.GroverTransferClient); ok {
		aware.SetMetricsCollector(collector)
	}

	jobs, err := buildUploadJobs(localPath, remotePath, dst.ExpectDirectory, info)
	if err != nil {
		return err
	}

	var (
		progress *output.FileProgressManager
		display  *output.MetricsDisplay
		summary  *output.TransferSummary
		done     atomic.Uint64
	)
	stopDisplay := func() {}
	stopProgress := func() {}
	if opts.uiMode() == "live" {
		if len(jobs) > 0 {
			fp := output.NewFileProgressManager("Uploads")
			if err := fp.Start(); err != nil {
				internal.Debug("unable to start upload progress", internal.Fields{
					internal.FieldError: err.Error(),
				})
			} else {
				progress = fp
				stopProgress = func() { progress.Stop() }
			}
		}

		display = output.NewMetricsDisplay("Network Telemetry", collector).WithInterval(opts.uiInterval())
		if progress != nil {
			if writer := progress.NewSection(); writer != nil {
				display = display.WithWriter(writer)
			}
		}
		if err := display.Start(cmd.Context()); err != nil {
			internal.Debug("unable to start metrics dashboard", internal.Fields{
				internal.FieldError: err.Error(),
			})
		} else {
			stopDisplay = func() { display.Stop() }
		}
	} else if opts.uiMode() == "summary" {
		summary = output.NewTransferSummary("upload", collector, len(jobs), totalUploadBytes(jobs), &done).
			WithInterval(opts.uiInterval()).
			WithWriter(cmd.OutOrStdout())
		if err := summary.Start(cmd.Context()); err != nil {
			internal.Debug("unable to start transfer summary", internal.Fields{
				internal.FieldError: err.Error(),
			})
		} else {
			stopDisplay = func() { summary.Stop() }
		}
	}
	cleanup := func() {
		stopDisplay()
		stopProgress()
	}
	defer cleanup()

	jobFns := make([]jobFunc, 0, len(jobs))
	for _, job := range jobs {
		job := job
		jobFns = append(jobFns, func(ctx context.Context) error {
			internal.Debug("uploading file", internal.Fields{
				"local":  job.localPath,
				"remote": job.remotePath,
			})
			f, err := os.Open(job.localPath)
			if err != nil {
				return fmt.Errorf("open %s: %w", job.localPath, err)
			}
			defer f.Close()

			reader := io.Reader(f)
			reader = wrapReaderWithDiskMetrics(reader, collector)
			if progress != nil {
				total := uint64(0)
				if job.size > 0 {
					total = uint64(job.size)
				}
				label := filepath.Base(job.localPath)
				if strings.TrimSpace(label) == "" {
					label = job.localPath
				}
				reader = progress.WrapReader(label, total, reader)
			}

			if err := transfer.Put(ctx, job.remotePath, reader, job.size, backend.ALWAYS); err != nil {
				return fmt.Errorf("upload %s -> %s: %w", job.localPath, job.remotePath, err)
			}
			done.Add(1)
			if opts.DeleteSource {
				if err := os.Remove(job.localPath); err != nil {
					return fmt.Errorf("remove source %s: %w", job.localPath, err)
				}
			}
			internal.Debug("upload complete", internal.Fields{
				"local":  job.localPath,
				"remote": job.remotePath,
			})
			return nil
		})
	}

	err = runJobs(cmd.Context(), opts.effectiveConcurrency(), jobFns)
	cleanup()
	cleanup = func() {}
	return err
}

func remotePathString(ref RemoteRef) string {
	pathPart := strings.TrimSpace(ref.Path)
	bucket := strings.TrimSpace(ref.Bucket)
	switch {
	case bucket == "":
		return pathPart
	case pathPart == "":
		return bucket
	default:
		return path.Join(bucket, pathPart)
	}
}

func expandUserPath(p string) (string, error) {
	p = strings.TrimSpace(p)
	if p == "" {
		return "", fmt.Errorf("path is required")
	}
	p = os.ExpandEnv(p)
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		p = filepath.Join(home, strings.TrimPrefix(p, "~"))
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func ensureParentDir(p string) error {
	dir := filepath.Dir(p)
	if dir == "" || dir == "." || dir == "/" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

type uploadJob struct {
	localPath  string
	remotePath string
	size       int64
}

type downloadJob struct {
	remotePath string
	localPath  string
	label      string
	size       uint64
}

func wrapReaderWithDiskMetrics(r io.Reader, collector *metrics.TransferCollector) io.Reader {
	if collector == nil || r == nil {
		return r
	}
	return &metricReader{
		reader: r,
		hook: func(n int) {
			collector.ObserveDiskRead(n)
		},
	}
}

func wrapWriterWithDiskMetrics(w io.Writer, collector *metrics.TransferCollector) io.Writer {
	if collector == nil || w == nil {
		return w
	}
	return &metricWriter{
		writer: w,
		hook: func(n int) {
			collector.ObserveDiskWrite(n)
		},
	}
}

type metricReader struct {
	reader io.Reader
	hook   func(int)
}

func (mr *metricReader) Read(p []byte) (int, error) {
	n, err := mr.reader.Read(p)
	if n > 0 && mr.hook != nil {
		mr.hook(n)
	}
	return n, err
}

func (mr *metricReader) ReadAt(p []byte, off int64) (int, error) {
	ra, ok := mr.reader.(io.ReaderAt)
	if !ok {
		return 0, fmt.Errorf("reader does not support ReadAt")
	}
	n, err := ra.ReadAt(p, off)
	if n > 0 && mr.hook != nil {
		mr.hook(n)
	}
	return n, err
}

type metricWriter struct {
	writer io.Writer
	hook   func(int)
}

func (mw *metricWriter) Write(p []byte) (int, error) {
	n, err := mw.writer.Write(p)
	if n > 0 && mw.hook != nil {
		mw.hook(n)
	}
	return n, err
}

func (opts CopyOptions) effectiveConcurrency() int {
	if opts.Concurrency <= 0 {
		return 1
	}
	return opts.Concurrency
}

func (opts CopyOptions) uiInterval() time.Duration {
	if opts.UIIntervalMs <= 0 {
		return 2 * time.Second
	}
	return time.Duration(opts.UIIntervalMs) * time.Millisecond
}

func (opts CopyOptions) uiMode() string {
	if opts.NoUI {
		return "none"
	}
	mode := strings.ToLower(strings.TrimSpace(opts.UIMode))
	if mode == "" {
		return "summary"
	}
	return mode
}

func (opts CopyOptions) validate() error {
	switch opts.uiMode() {
	case "summary", "live", "none":
	default:
		return fmt.Errorf("invalid --ui %q: must be summary, live, or none", opts.UIMode)
	}
	if opts.ParallelStreams < 0 {
		return fmt.Errorf("--parallel-streams must be >= 0")
	}
	if opts.UDPWindowPackets < 0 || opts.UDPWindowBytes < 0 || opts.UDPAckEveryPackets < 0 || opts.UDPAckEveryMs < 0 {
		return fmt.Errorf("udp tuning values must be >= 0")
	}
	switch strings.ToLower(strings.TrimSpace(opts.UDPFlowControl)) {
	case "", "fixed", "bbr":
	default:
		return fmt.Errorf("invalid --udp-flow-control %q: must be fixed or bbr", opts.UDPFlowControl)
	}
	if mtu := strings.TrimSpace(opts.MTU); mtu != "" && !strings.EqualFold(mtu, "auto") {
		var parsed int
		if _, err := fmt.Sscanf(mtu, "%d", &parsed); err != nil || parsed <= 0 {
			return fmt.Errorf("invalid --mtu %q: must be auto or a positive integer", opts.MTU)
		}
	}
	return nil
}

func totalUploadBytes(jobs []uploadJob) uint64 {
	var total uint64
	for _, job := range jobs {
		if job.size > 0 {
			total += uint64(job.size)
		}
	}
	return total
}

func totalDownloadBytes(jobs []downloadJob) uint64 {
	var total uint64
	for _, job := range jobs {
		total += job.size
	}
	return total
}

func buildUploadJobs(localRoot string, remoteBase string, destIsDir bool, info os.FileInfo) ([]uploadJob, error) {
	if info.IsDir() {
		jobs := []uploadJob{}
		err := filepath.WalkDir(localRoot, func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			entryInfo, err := d.Info()
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(localRoot, p)
			if err != nil {
				return err
			}
			remotePath := path.Join(remoteBase, filepath.ToSlash(rel))
			if strings.TrimSpace(remotePath) == "" {
				return fmt.Errorf("unable to derive remote path for %s", p)
			}
			jobs = append(jobs, uploadJob{
				localPath:  p,
				remotePath: remotePath,
				size:       entryInfo.Size(),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
		if len(jobs) == 0 {
			return nil, fmt.Errorf("no files found under %s", localRoot)
		}
		return jobs, nil
	}

	target := strings.TrimSpace(remoteBase)
	if destIsDir || target == "" || target == "." {
		target = path.Join(remoteBase, filepath.Base(localRoot))
	}
	if strings.TrimSpace(target) == "" || target == "." {
		return nil, fmt.Errorf("remote destination is missing a path")
	}
	return []uploadJob{{
		localPath:  localRoot,
		remotePath: target,
		size:       info.Size(),
	}}, nil
}

type jobFunc func(context.Context) error

func runJobs(ctx context.Context, concurrency int, jobs []jobFunc) error {
	if concurrency < 1 {
		concurrency = 1
	}
	if len(jobs) == 0 {
		return nil
	}

	sem := make(chan struct{}, concurrency)
	errCh := make(chan error, len(jobs))
	var wg sync.WaitGroup

	for _, job := range jobs {
		if job == nil {
			continue
		}
		wg.Add(1)
		go func(fn jobFunc) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
			defer func() { <-sem }()

			if err := fn(ctx); err != nil {
				errCh <- err
			}
		}(job)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func resolveDownloadDestination(dst RemoteRef, multi bool) (string, bool, error) {
	localPath := strings.TrimSpace(dst.Path)
	if localPath == "" {
		return "", false, fmt.Errorf("destination path is required")
	}
	localPath, err := expandUserPath(localPath)
	if err != nil {
		return "", false, err
	}

	info, statErr := os.Stat(localPath)
	switch {
	case statErr == nil && info.IsDir():
		return localPath, true, nil
	case statErr == nil:
		if multi || dst.ExpectDirectory {
			return "", false, fmt.Errorf("destination %q must be a directory", localPath)
		}
		return localPath, false, nil
	case os.IsNotExist(statErr):
		if multi || dst.ExpectDirectory {
			if err := os.MkdirAll(localPath, 0o755); err != nil {
				return "", false, err
			}
			return localPath, true, nil
		}
		if err := ensureParentDir(localPath); err != nil {
			return "", false, err
		}
		return localPath, false, nil
	default:
		return "", false, statErr
	}
}

func newTransferClientForRemote(cmd *cobra.Command, ref RemoteRef, opts CopyOptions) (*gclient.Client, error) {
	cfg := GetAppConfig(cmd)
	if protocol, _ := cmd.Flags().GetString("protocol"); strings.TrimSpace(protocol) != "" {
		cfg.TransferProtocol = strings.ToLower(strings.TrimSpace(protocol))
	}
	if p := strings.ToLower(strings.TrimSpace(cfg.TransferProtocol)); p != "" && p != "udp" && p != "tcp" {
		return nil, fmt.Errorf("invalid transfer protocol %q: must be udp or tcp", cfg.TransferProtocol)
	}
	serverURLFromFlag := cmd.Root().PersistentFlags().Changed("server-url")
	if name := strings.TrimSpace(ref.RemoteName); name != "" && !serverURLFromFlag {
		var (
			cred backend.Credential
			err  error
		)
		if parsed, parseErr := uuid.Parse(name); parseErr == nil {
			cred, err = loadCredentialByRef(cfg, "", parsed)
		} else {
			cred, err = loadCredentialByRef(cfg, name, uuid.Nil)
		}
		if err != nil {
			return nil, fmt.Errorf("load credential %q: %w", ref.RemoteName, err)
		}
		basic, ok := cred.(*backend.BasicAuthCredential)
		if !ok {
			return nil, fmt.Errorf("credential %q must be a basic credential to connect to a grover server", cred.GetName())
		}
		cfg.ServerURL = basic.GetUrl()
	}
	if strings.TrimSpace(cfg.ServerURL) == "" {
		return nil, fmt.Errorf("server URL is not configured; set --server-url or provide a credential reference")
	}

	client := gclient.NewClient(*cfg)
	if err := client.Initialize(cmd.Context(), util.RouteForceRemote); err != nil {
		return nil, err
	}

	if client.Transfer() == nil {
		_ = client.Close()
		return nil, fmt.Errorf("transfer API not available on remote server")
	}
	if aware, ok := client.Transfer().(*gclient.GroverTransferClient); ok {
		aware.ApplyTransferOptions(gclient.TransferOptions{
			ParallelStreams:    opts.ParallelStreams,
			UDPFlowControl:     opts.UDPFlowControl,
			UDPWindowPackets:   opts.UDPWindowPackets,
			UDPWindowBytes:     opts.UDPWindowBytes,
			UDPAckEveryPackets: opts.UDPAckEveryPackets,
			UDPAckEveryMs:      opts.UDPAckEveryMs,
			MTU:                opts.MTU,
			AutoMTU:            strings.EqualFold(strings.TrimSpace(opts.MTU), "auto"),
		})
	}
	return client, nil
}

func DownloadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "download <remote_source> <local_path>",
		Short:        "Download a single file from a grover server",
		Args:         cobra.ExactArgs(2),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			src, err := parseLocation(args[0])
			if err != nil {
				return err
			}
			if !src.isRemote {
				return fmt.Errorf("source %q is not a remote reference (remote:path)", args[0])
			}

			dst, err := parseLocation(args[1])
			if err != nil {
				return err
			}
			if dst.isRemote {
				return fmt.Errorf("destination must be a local path when downloading")
			}

			opts := CopyOptions{Concurrency: 1}
			printer := output.NewPrinter()
			return downloadFromRemote(cmd, src, dst, opts, printer)
		},
	}
	return cmd
}
