package output

import (
	"fmt"

	"github.com/jgoldverg/grover/backend"
	"github.com/jgoldverg/grover/backend/filesystem"
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

func PrintFileTable(files []filesystem.FileInfo) error {
	tableData := [][]string{
		{"Abs Path", "Size"},
	}

	for _, f := range files {
		sizeStr := humanizeSize(f.Size)
		tableData = append(tableData, []string{f.AbsPath, sizeStr})
	}

	return pterm.DefaultTable.WithHasHeader().WithData(tableData).Render()
}

func VisualizeCredentialList(credList []backend.Credential) {
	for _, cred := range credList {
		pterm.Printfln("[%s]", cred.GetName())
		pterm.Printfln("type = %s", cred.GetType())
		pterm.Printfln("url = %s", cred.GetUrl())
		pterm.Printfln("uuid = %s", cred.GetUUID())
		switch c := cred.(type) {
		case *backend.SSHCredential:
			pterm.Printfln("username = %s", c.Username)
			pterm.Printfln("private-key-path = %s", c.PrivateKeyPath)
			pterm.Printfln("public-key-path = %s", c.PublicKeyPath)
			pterm.Printfln("ssh-agent = %t", c.UseAgent)
		case *backend.BasicAuthCredential:
			pterm.Printfln("username = %s", c.Username)
			pterm.Printfln("password = %s", c.Password)
			pterm.Println() // Empty line between credentials
		}
		pterm.Println("")
	}
}
