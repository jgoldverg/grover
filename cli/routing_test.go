package cli

import (
	"testing"

	"github.com/jgoldverg/grover/pkg/util"
	"github.com/spf13/cobra"
)

func TestResolveRoutePolicyUsesExecutionFlag(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("execution", "", "")
	if err := cmd.Flags().Set("execution", "server"); err != nil {
		t.Fatal(err)
	}

	if got := resolveRoutePolicy(cmd, "client"); got != util.RouteForceRemote {
		t.Fatalf("policy = %v, want remote", got)
	}
}

func TestMetadataCommandsDoNotUseViaFlag(t *testing.T) {
	for name, cmd := range map[string]*cobra.Command{
		"credential": CredentialCommand(),
		"backend":    BackendCommand(),
	} {
		if flag := cmd.Flag("via"); flag != nil {
			t.Fatalf("%s still exposes old --via execution flag", name)
		}
		if flag := cmd.Flag("execution"); flag == nil {
			t.Fatalf("%s does not expose --execution", name)
		}
	}
}
