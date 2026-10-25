package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type transferHistoryOptions struct {
	LogDir string
	JSON   bool
}

type transferHistoryManifest struct {
	JobID           string    `json:"job_id"`
	RouteID         string    `json:"route_id"`
	Protocol        string    `json:"protocol"`
	SourceRoot      string    `json:"source_root"`
	DestinationRoot string    `json:"destination_root"`
	TotalFiles      int       `json:"total_files"`
	TotalBytes      uint64    `json:"total_bytes"`
	CreatedAt       time.Time `json:"created_at"`
}

type transferHistoryFinal struct {
	JobID        string `json:"jobId"`
	RouteID      string `json:"routeId"`
	State        string `json:"state"`
	ErrorMessage string `json:"errorMessage"`
	GoodBytes    string `json:"goodBytes"`
	NetworkBytes string `json:"networkBytes"`
}

type transferHistoryEntry struct {
	Path     string                  `json:"path"`
	Manifest transferHistoryManifest `json:"manifest"`
	Final    *transferHistoryFinal   `json:"final,omitempty"`
}

func TransferHistoryCommand() *cobra.Command {
	opts := transferHistoryOptions{}
	cmd := &cobra.Command{
		Use:          "history [job_id]",
		Short:        "Inspect local groverd job log directories",
		Args:         cobra.MaximumNArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logDir := strings.TrimSpace(opts.LogDir)
			if logDir == "" {
				logDir = "/var/log/grover"
			}
			if len(args) == 1 {
				entry, err := readTransferHistoryJob(logDir, args[0])
				if err != nil {
					return err
				}
				if opts.JSON {
					return writeJSON(cmd.OutOrStdout(), entry)
				}
				printTransferHistoryEntry(cmd.OutOrStdout(), entry)
				return nil
			}
			entries, err := listTransferHistory(logDir)
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), struct {
					Jobs []transferHistoryEntry `json:"jobs"`
				}{Jobs: entries})
			}
			if len(entries) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no transfer history")
				return nil
			}
			for _, entry := range entries {
				state := "-"
				if entry.Final != nil {
					state = entry.Final.State
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\t%d\t%s\n",
					entry.Manifest.JobID,
					entry.Manifest.RouteID,
					state,
					entry.Manifest.TotalBytes,
					entry.Path,
				)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.LogDir, "job-log-dir", "", "Local groverd job log directory")
	cmd.Flags().BoolVar(&opts.JSON, "json", false, "Print JSON")
	return cmd
}

func listTransferHistory(logDir string) ([]transferHistoryEntry, error) {
	dirs, err := os.ReadDir(logDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	entries := []transferHistoryEntry{}
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		entry, err := readTransferHistoryDir(filepath.Join(logDir, dir.Name()))
		if err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Manifest.CreatedAt.After(entries[j].Manifest.CreatedAt)
	})
	return entries, nil
}

func readTransferHistoryJob(logDir, jobID string) (transferHistoryEntry, error) {
	safe := safeSchedulePathPart(jobID)
	entry, err := readTransferHistoryDir(filepath.Join(logDir, safe))
	if err == nil {
		return entry, nil
	}
	entries, listErr := listTransferHistory(logDir)
	if listErr != nil {
		return transferHistoryEntry{}, listErr
	}
	for _, entry := range entries {
		if entry.Manifest.JobID == jobID {
			return entry, nil
		}
	}
	return transferHistoryEntry{}, err
}

func readTransferHistoryDir(dir string) (transferHistoryEntry, error) {
	manifestPath := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return transferHistoryEntry{}, err
	}
	var manifest transferHistoryManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return transferHistoryEntry{}, fmt.Errorf("read %s: %w", manifestPath, err)
	}
	entry := transferHistoryEntry{Path: dir, Manifest: manifest}
	finalPath := filepath.Join(dir, "final.json")
	if data, err := os.ReadFile(finalPath); err == nil {
		var final transferHistoryFinal
		if err := json.Unmarshal(data, &final); err == nil {
			entry.Final = &final
		}
	}
	return entry, nil
}

func printTransferHistoryEntry(w io.Writer, entry transferHistoryEntry) {
	fmt.Fprintf(w, "job_id: %s\n", entry.Manifest.JobID)
	fmt.Fprintf(w, "route_id: %s\n", entry.Manifest.RouteID)
	fmt.Fprintf(w, "protocol: %s\n", entry.Manifest.Protocol)
	fmt.Fprintf(w, "source_root: %s\n", entry.Manifest.SourceRoot)
	fmt.Fprintf(w, "destination_root: %s\n", entry.Manifest.DestinationRoot)
	fmt.Fprintf(w, "total_files: %d\n", entry.Manifest.TotalFiles)
	fmt.Fprintf(w, "total_bytes: %d\n", entry.Manifest.TotalBytes)
	fmt.Fprintf(w, "path: %s\n", entry.Path)
	if entry.Final != nil {
		fmt.Fprintf(w, "state: %s\n", entry.Final.State)
		fmt.Fprintf(w, "good_bytes: %s\n", entry.Final.GoodBytes)
		fmt.Fprintf(w, "network_bytes: %s\n", entry.Final.NetworkBytes)
		if entry.Final.ErrorMessage != "" {
			fmt.Fprintf(w, "error: %s\n", entry.Final.ErrorMessage)
		}
	}
}
