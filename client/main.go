package main

import (
	"log"

	"github.com/jgoldverg/grover/client/cmd"
)

func main() {
	rootCmd := cmd.NewRootCommand()

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v\n", err)
	}
}
