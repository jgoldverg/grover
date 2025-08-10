package backend

import (
	"github.com/jgoldverg/grover/backend/fs"
	"github.com/jgoldverg/grover/backend/localfs"
	"github.com/jgoldverg/grover/config"
)

func ListerFactory(kind fs.ListerType, config config.ListerConfig) fs.FileLister {
	if kind == fs.ListerFilesystem {
		return localfs.NewFileSystemLister()
	}
	return nil
}
