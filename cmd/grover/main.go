package main

import (
	"log"

	"github.com/jgoldverg/grover/cli"
)

func main() {
	rootCmd := cli.NewRootCommand()

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v\n", err)
	}
}
