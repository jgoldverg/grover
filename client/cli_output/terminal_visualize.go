package cli_output

import (
	"fmt"

	"github.com/jgoldverg/grover/backend/fs"
	"github.com/pterm/pterm"
)

func humanizeSize(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func PrintFileTable(files []fs.FileInfo) error {
	tableData := [][]string{
		{"Abs Path", "Size"},
	}

	for _, f := range files {
		sizeStr := humanizeSize(f.Size)
		tableData = append(tableData, []string{f.AbsPath, sizeStr})
	}

	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}
