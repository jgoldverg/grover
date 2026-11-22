package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// FilePlan is one file to move. RelativePath is slash-separated and is joined
// under the job's destination directory on the receiving node.
type FilePlan struct {
	SourcePath   string // absolute path, or a synthetic:// URL
	RelativePath string
	Size         uint64
}

const syntheticScheme = "synthetic"

// PlanSources expands source arguments into file plans.
//
//   - A regular file becomes <destination>/<basename>.
//   - A directory's contents are copied under <destination>/, preserving the
//     layout below the directory (like `cp -r dir/. dest/`).
//   - synthetic://<bytes>/<rel/path> produces <bytes> zero bytes at
//     <destination>/<rel/path> without reading the disk.
func PlanSources(ctx context.Context, sources []string) ([]FilePlan, error) {
	if len(sources) == 0 {
		return nil, errors.New("at least one source path is required")
	}
	var plans []FilePlan
	for _, src := range sources {
		src = strings.TrimSpace(src)
		if s, ok, err := parseSynthetic(src); ok || err != nil {
			if err != nil {
				return nil, err
			}
			plans = append(plans, s)
			continue
		}
		if !filepath.IsAbs(src) {
			return nil, fmt.Errorf("source path %q must be absolute", src)
		}
		src = filepath.Clean(src)
		info, err := os.Stat(src)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			plans = append(plans, FilePlan{SourcePath: src, RelativePath: filepath.Base(src), Size: uint64(info.Size())})
			continue
		}
		err = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !d.Type().IsRegular() {
				return nil // skip directories, symlinks, devices
			}
			fi, err := d.Info()
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(src, p)
			if err != nil {
				return err
			}
			plans = append(plans, FilePlan{SourcePath: p, RelativePath: filepath.ToSlash(rel), Size: uint64(fi.Size())})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if len(plans) == 0 {
		return nil, errors.New("no files to transfer")
	}
	return plans, nil
}

// SyntheticSource builds a synthetic source URL.
func SyntheticSource(size uint64, relPath string) string {
	parts := strings.Split(path.Clean("/" + relPath)[1:], "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return fmt.Sprintf("%s://%d/%s", syntheticScheme, size, strings.Join(parts, "/"))
}

// IsSynthetic reports whether a source is a synthetic payload.
func IsSynthetic(src string) bool {
	return strings.HasPrefix(strings.TrimSpace(src), syntheticScheme+"://")
}

func parseSynthetic(raw string) (FilePlan, bool, error) {
	if !IsSynthetic(raw) {
		return FilePlan{}, false, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return FilePlan{}, true, fmt.Errorf("invalid synthetic source %q: %w", raw, err)
	}
	size, err := strconv.ParseUint(u.Host, 10, 64)
	if err != nil {
		return FilePlan{}, true, fmt.Errorf("invalid synthetic size in %q", raw)
	}
	rel := strings.TrimPrefix(u.Path, "/")
	if rel == "" {
		rel = fmt.Sprintf("synthetic-%d.bin", size)
	}
	if err := checkRelativePath(rel); err != nil {
		return FilePlan{}, true, err
	}
	return FilePlan{SourcePath: raw, RelativePath: rel, Size: size}, true, nil
}

// openRange opens [offset, offset+length) of a planned file for reading.
func openRange(p FilePlan, offset, length int64) (io.ReadCloser, error) {
	if IsSynthetic(p.SourcePath) {
		return io.NopCloser(io.LimitReader(zeroReader{}, length)), nil
	}
	f, err := os.Open(p.SourcePath)
	if err != nil {
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{io.NewSectionReader(f, offset, length), f}, nil
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// checkRelativePath rejects paths that could escape the destination directory.
func checkRelativePath(rel string) error {
	if rel == "" || strings.HasPrefix(rel, "/") || strings.Contains(rel, "\\") {
		return fmt.Errorf("invalid relative path %q", rel)
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid relative path %q", rel)
		}
	}
	return nil
}

// DestinationPath joins a relative transfer path under an absolute destination
// directory, refusing anything that would escape it.
func DestinationPath(destination, rel string) (string, error) {
	if !filepath.IsAbs(destination) {
		return "", fmt.Errorf("destination %q must be an absolute path", destination)
	}
	if err := checkRelativePath(rel); err != nil {
		return "", err
	}
	return filepath.Join(filepath.Clean(destination), filepath.FromSlash(rel)), nil
}

// byteRange is a slice of a file handled by one stream.
type byteRange struct {
	offset int64
	length int64
}

// splitRanges divides size bytes into at most n near-equal ranges. A zero-size
// file still gets one empty range so it is created on the destination.
func splitRanges(size int64, n int) []byteRange {
	if n < 1 {
		n = 1
	}
	if size <= 0 {
		return []byteRange{{0, 0}}
	}
	if int64(n) > size {
		n = int(size)
	}
	out := make([]byteRange, 0, n)
	base, rem := size/int64(n), size%int64(n)
	var off int64
	for i := 0; i < n; i++ {
		l := base
		if int64(i) < rem {
			l++
		}
		out = append(out, byteRange{off, l})
		off += l
	}
	return out
}
