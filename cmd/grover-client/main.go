package main

import (
	"log"

	cmd "github.com/jgoldverg/grover/cmd/grover-client/cmd"
)

func main() {
	rootCmd := cmd.NewRootCommand()

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v\n", err)
	}
}
