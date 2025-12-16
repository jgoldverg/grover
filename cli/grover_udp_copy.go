package cli

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

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
				return downloadFromRemote(cmd, src, dst)
			default:
				return uploadToRemote(cmd, src, dst, opts.DeleteSource)
			}
		},
	}
	cmd.Flags().BoolVar(&opts.DeleteSource, "delete-source", false, "Delete the local source file after a successful upload")
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

func downloadFromRemote(cmd *cobra.Command, src RemoteRef, dst RemoteRef) error {
	if dst.isRemote {
		return fmt.Errorf("destination must be local when downloading")
	}
	if src.ExpectDirectory {
		return fmt.Errorf("source %q refers to a directory; specify a file to download", src.Raw)
	}
	remotePath := remotePathString(src)
	if strings.TrimSpace(remotePath) == "" {
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

	localPath, err := prepareLocalDestination(dst, remotePath)
	if err != nil {
		return err
	}
	if err := ensureParentDir(localPath); err != nil {
		return err
	}

	out, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer out.Close()

	if err := transfer.Get(cmd.Context(), remotePath, out); err != nil {
		return err
	}
	return nil
}

func uploadToRemote(cmd *cobra.Command, src RemoteRef, dst RemoteRef, deleteSource bool) error {
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
	if info.IsDir() {
		return fmt.Errorf("source %q is a directory; directory transfers are not supported yet", localPath)
	}

	remotePath := remotePathString(dst)
	if dst.ExpectDirectory || remotePath == "" {
		filename := filepath.Base(localPath)
		remotePath = path.Join(remotePath, filename)
	}
	if strings.TrimSpace(remotePath) == "" || remotePath == "." {
		return fmt.Errorf("remote destination %q is missing a path", dst.Raw)
	}

	internal.Info("resolved transfer paths", internal.Fields{
		"local_path":    localPath,
		"remote_path":   remotePath,
		"delete_source": deleteSource,
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
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	internal.Info("streaming file to server", internal.Fields{
		"remote_path": remotePath,
		"bytes":       info.Size(),
	})
	if err := transfer.Put(cmd.Context(), remotePath, f, info.Size(), backend.ALWAYS); err != nil {
		return err
	}
	internal.Info("upload complete", internal.Fields{
		"remote_path": remotePath,
		"bytes":       info.Size(),
	})
	if deleteSource {
		if err := os.Remove(localPath); err != nil {
			return fmt.Errorf("remove source %q: %w", localPath, err)
		}
	}
	return nil
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

func prepareLocalDestination(dst RemoteRef, remotePath string) (string, error) {
	if strings.TrimSpace(dst.Path) == "" {
		return "", fmt.Errorf("destination path is required")
	}
	localPath, err := expandUserPath(dst.Path)
	if err != nil {
		return "", err
	}

	wantDir := dst.ExpectDirectory
	info, statErr := os.Stat(localPath)
	switch {
	case statErr == nil && info.IsDir():
		wantDir = true
	case statErr == nil:
		wantDir = false
	case os.IsNotExist(statErr):
		if dst.ExpectDirectory {
			if err := os.MkdirAll(localPath, 0o755); err != nil {
				return "", err
			}
			wantDir = true
		}
	default:
		return "", statErr
	}

	if wantDir {
		filename := path.Base(remotePath)
		if filename == "" || filename == "." || filename == "/" {
			return "", fmt.Errorf("remote path %q does not include a file name; specify a destination file", remotePath)
		}
		return filepath.Join(localPath, filename), nil
	}

	return localPath, nil
}

func ensureParentDir(p string) error {
	dir := filepath.Dir(p)
	if dir == "" || dir == "." || dir == "/" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
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
