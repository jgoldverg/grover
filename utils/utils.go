package utils

import (
	"os"
	"path/filepath"
)

func EnsureFileExists(path string) error {
	dir := filepath.Dir(path)

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Check if the file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Create the file
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
	}

	return nil
}
