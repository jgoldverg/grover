package log

import (
	"fmt"
	"time"

	"github.com/pterm/pterm"
)

type FieldKey string

const (
	FieldError            FieldKey = "error"
	FieldMsg              FieldKey = "message"
	FieldRtt              FieldKey = "rtt"
	FieldServer           FieldKey = "server"
	FieldPort             FieldKey = "port"
	ServerCertificatePath FieldKey = "certificate_path"
	ServerKeyPath         FieldKey = "key_path"
	CredentialPath        FieldKey = "credential_path"
	ConfigPath            FieldKey = "config_path"
)

type Fields map[FieldKey]interface{}

func Structured(printer *pterm.PrefixPrinter, msg string, fields Fields) {
	timestamp := time.Now().Format(time.RFC3339)

	fieldStr := ""
	for k, v := range fields {
		fieldStr += fmt.Sprintf("%s=%v ", k, v)
	}

	printer.Printfln("[%s] %s %s", timestamp, msg, fieldStr)
}
