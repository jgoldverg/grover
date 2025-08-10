package main

import (
	"github.com/jgoldverg/GoRover/backend"
	"github.com/jgoldverg/GoRover/client/cmd"
	"github.com/jgoldverg/GoRover/config"
	"log"
)

func main() {
	ac := config.NewAutoConfig()
	credStorage, err := backend.NewTomlCredentialStorage(ac.TomlFilePath)
	if err != nil {
		log.Fatalf("Failed to create credential storage: %v", err)
	}
	rootCmd := cmd.NewRootCommand(credStorage, ac)
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("error: %v\n", err)
	}
}
