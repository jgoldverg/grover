package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	pb "github.com/jgoldverg/grover/pkg/groverpb/groverv1"
)

const syntheticTransferScheme = "synthetic"

// FilesystemSourcePlanner resolves local paths and synthetic payload descriptions.
type FilesystemSourcePlanner struct{}

func (FilesystemSourcePlanner) PlanFiles(ctx context.Context, source *pb.TransferEndpoint, paths []string) ([]FilePlan, error) {
	root := filepath.Clean(source.GetRootPath())
	var plans []FilePlan
	relativePathForFile := func(full string) (string, error) {
		rel, err := filepath.Rel(root, full)
		if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
			rel = filepath.Base(full)
		}
		rel = filepath.Clean(rel)
		if rel == "." || filepath.IsAbs(rel) || strings.HasPrefix(rel, "..") {
			return "", fmt.Errorf("invalid transfer relative path %q for %q", rel, full)
		}
		return filepath.ToSlash(rel), nil
	}
	addPath := func(p string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if synthetic, ok, err := parseSyntheticTransferSource(p); ok || err != nil {
			if err != nil {
				return err
			}
			plans = append(plans, FilePlan{
				SourcePath:   p,
				RelativePath: synthetic.RelativePath,
				Size:         synthetic.Size,
			})
			return nil
		}
		full := p
		if !filepath.IsAbs(full) {
			full = filepath.Join(root, full)
		}
		info, err := os.Stat(full)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return filepath.WalkDir(full, func(path string, d os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if d.IsDir() {
					return nil
				}
				info, err := d.Info()
				if err != nil {
					return err
				}
				rel, err := relativePathForFile(path)
				if err != nil {
					return err
				}
				plans = append(plans, FilePlan{SourcePath: path, RelativePath: rel, Size: uint64(info.Size())})
				return nil
			})
		}
		rel, err := relativePathForFile(full)
		if err != nil {
			return err
		}
		plans = append(plans, FilePlan{SourcePath: full, RelativePath: rel, Size: uint64(info.Size())})
		return nil
	}
	if len(paths) == 0 {
		if err := addPath(root); err != nil {
			return nil, err
		}
		return plans, nil
	}
	for _, p := range paths {
		if err := addPath(strings.TrimSpace(p)); err != nil {
			return nil, err
		}
	}
	return plans, nil
}

func parseSyntheticTransferSource(raw string) (syntheticTransferSource, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return syntheticTransferSource{}, false, nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != syntheticTransferScheme {
		return syntheticTransferSource{}, false, nil
	}
	size, err := parseSyntheticSize(u.Host)
	if err != nil {
		return syntheticTransferSource{}, true, err
	}
	rel := strings.TrimPrefix(u.EscapedPath(), "/")
	if rel == "" {
		rel = fmt.Sprintf("synthetic-%d.bin", size)
	}
	if decoded, err := url.PathUnescape(rel); err == nil {
		rel = decoded
	}
	rel = filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(rel) || rel == "." || strings.HasPrefix(rel, "..") {
		return syntheticTransferSource{}, true, fmt.Errorf("invalid synthetic transfer path %q", rel)
	}
	return syntheticTransferSource{RelativePath: filepath.ToSlash(rel), Size: size}, true, nil
}

func parseSyntheticSize(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("synthetic transfer size is required")
	}
	var size uint64
	if _, err := fmt.Sscanf(raw, "%d", &size); err != nil {
		return 0, fmt.Errorf("invalid synthetic transfer size %q", raw)
	}
	return size, nil
}

func openTransferPlanReader(plan FilePlan, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("invalid transfer reader range: offset=%d length=%d", offset, length)
	}
	if _, ok, err := parseSyntheticTransferSource(plan.SourcePath); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return io.NopCloser(io.LimitReader(zeroReader{}, length)), nil
	}
	src, err := os.Open(plan.SourcePath)
	if err != nil {
		return nil, err
	}
	if offset == 0 {
		return struct {
			io.Reader
			io.Closer
		}{Reader: io.LimitReader(src, length), Closer: src}, nil
	}
	if _, err := src.Seek(offset, io.SeekStart); err != nil {
		_ = src.Close()
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{Reader: io.LimitReader(src, length), Closer: src}, nil
}

func transferPlanReadsDisk(plan FilePlan) bool {
	_, ok, err := parseSyntheticTransferSource(plan.SourcePath)
	return err == nil && !ok
}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
