package cli

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/internal"
	"github.com/jgoldverg/grover/pkg/gclient"
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
	DeleteSource bool
	Concurrency  int
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
				return downloadFromRemote(cmd, src, dst, opts)
			default:
				return uploadToRemote(cmd, src, dst, opts)
			}
		},
	}
	cmd.Flags().BoolVar(&opts.DeleteSource, "delete-source", false, "Delete the local source file after a successful upload")
	cmd.Flags().IntVar(&opts.Concurrency, "concurrency", 4, "Maximum number of files to transfer in parallel")
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

func downloadFromRemote(cmd *cobra.Command, src RemoteRef, dst RemoteRef, opts CopyOptions) error {
	if dst.isRemote {
		return fmt.Errorf("destination must be local when downloading")
	}

	remoteRoot := remotePathString(src)
	if strings.TrimSpace(remoteRoot) == "" {
		return fmt.Errorf("remote source %q is missing a path", src.Raw)
	}

	client, err := newTransferClientForRemote(cmd, src)
	if err != nil {
		return err
	}
	defer client.Close()

	transfer := client.Transfer()
	if transfer == nil {
		return fmt.Errorf("transfer service unavailable on remote server")
	}

	files, err := transfer.Enumerate(cmd.Context(), remoteRoot, true)
	if err != nil {
		return err
	}
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
		jobs = append(jobs, downloadJob{
			remotePath: rf.FullPath,
			localPath:  localTarget,
		})
	}

	return runDownloadJobs(cmd.Context(), transfer, jobs, opts.effectiveConcurrency())
}

func uploadToRemote(cmd *cobra.Command, src RemoteRef, dst RemoteRef, opts CopyOptions) error {
	if !dst.isRemote {
		return fmt.Errorf("destination must be remote when uploading")
	}
	if src.isRemote {
		return fmt.Errorf("source must be local when uploading")
	}
	internal.Info("starting upload", internal.Fields{
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

	internal.Info("resolved transfer paths", internal.Fields{
		"local_path":    localPath,
		"remote_path":   remotePath,
		"delete_source": opts.DeleteSource,
	})
	client, err := newTransferClientForRemote(cmd, dst)
	if err != nil {
		return err
	}
	defer client.Close()

	transfer := client.Transfer()
	if transfer == nil {
		return fmt.Errorf("transfer service unavailable on remote server")
	}

	jobs, err := buildUploadJobs(localPath, remotePath, dst.ExpectDirectory, info)
	if err != nil {
		return err
	}
	return runUploadJobs(cmd.Context(), transfer, jobs, opts.effectiveConcurrency(), backend.ALWAYS, opts.DeleteSource)
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
}

func (opts CopyOptions) effectiveConcurrency() int {
	if opts.Concurrency <= 0 {
		return 1
	}
	return opts.Concurrency
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

func runUploadJobs(ctx context.Context, transfer gclient.TransferAPI, jobs []uploadJob, concurrency int, overwrite backend.OverwritePolicy, deleteSource bool) error {
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	errCh := make(chan error, len(jobs))
	var wg sync.WaitGroup

	for _, job := range jobs {
		job := job
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
			defer func() { <-sem }()

			f, err := os.Open(job.localPath)
			if err != nil {
				errCh <- fmt.Errorf("open %s: %w", job.localPath, err)
				return
			}
			defer f.Close()

			if err := transfer.Put(ctx, job.remotePath, f, job.size, overwrite); err != nil {
				errCh <- fmt.Errorf("upload %s -> %s: %w", job.localPath, job.remotePath, err)
				return
			}
			if deleteSource {
				if err := os.Remove(job.localPath); err != nil {
					errCh <- fmt.Errorf("remove source %s: %w", job.localPath, err)
					return
				}
			}
		}()
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

func runDownloadJobs(ctx context.Context, transfer gclient.TransferAPI, jobs []downloadJob, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	errCh := make(chan error, len(jobs))
	var wg sync.WaitGroup

	for _, job := range jobs {
		job := job
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
			defer func() { <-sem }()

			if err := ensureParentDir(job.localPath); err != nil {
				errCh <- err
				return
			}
			out, err := os.Create(job.localPath)
			if err != nil {
				errCh <- fmt.Errorf("create %s: %w", job.localPath, err)
				return
			}
			defer out.Close()

			if err := transfer.Get(ctx, job.remotePath, out); err != nil {
				errCh <- fmt.Errorf("download %s -> %s: %w", job.remotePath, job.localPath, err)
				return
			}
		}()
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

func newTransferClientForRemote(cmd *cobra.Command, ref RemoteRef) (*gclient.Client, error) {
	cfg := GetAppConfig(cmd)
	if name := strings.TrimSpace(ref.RemoteName); name != "" {
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
			return downloadFromRemote(cmd, src, dst, opts)
		},
	}
	return cmd
}
