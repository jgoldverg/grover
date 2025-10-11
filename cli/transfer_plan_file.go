package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type planDocument struct {
	Version        int           `json:"version" yaml:"version"`
	Via            string        `json:"via" yaml:"via"`
	Params         *planParams   `json:"params" yaml:"params"`
	IdempotencyKey string        `json:"idempotency_key" yaml:"idempotency_key"`
	Endpoints      planEndpoints `json:"endpoints" yaml:"endpoints"`
	Maps           []planMap     `json:"maps" yaml:"maps"`
}

type planEndpoints struct {
	From []planEndpoint `json:"from" yaml:"from"`
	To   []planEndpoint `json:"to" yaml:"to"`
}

type planEndpoint struct {
	Name           string `json:"name" yaml:"name"`
	URI            string `json:"uri" yaml:"uri"`
	CredentialID   string `json:"credential_id" yaml:"credential_id"`
	CredentialHint string `json:"credential_hint" yaml:"credential_hint"`
}

type planParams struct {
	Concurrency    *uint   `json:"concurrency" yaml:"concurrency"`
	Parallelism    *uint   `json:"parallelism" yaml:"parallelism"`
	Pipelining     *uint   `json:"pipelining" yaml:"pipelining"`
	ChunkSize      *uint64 `json:"chunk_size" yaml:"chunk_size"`
	RateLimitMbps  *uint   `json:"rate_limit_mbps" yaml:"rate_limit_mbps"`
	VerifyChecksum *bool   `json:"verify_checksum" yaml:"verify_checksum"`
	MaxRetries     *uint   `json:"max_retries" yaml:"max_retries"`
	RetryBackoffMs *uint   `json:"retry_backoff_ms" yaml:"retry_backoff_ms"`
	Overwrite      *string `json:"overwrite" yaml:"overwrite"`
	Checksum       *string `json:"checksum" yaml:"checksum"`
}

type planMap struct {
	From       stringList        `json:"from" yaml:"from"`
	To         stringList        `json:"to" yaml:"to"`
	SourcePath string            `json:"source_path" yaml:"source_path"`
	DestPath   string            `json:"dest_path" yaml:"dest_path"`
	Options    map[string]string `json:"options" yaml:"options"`
}

type stringList []string

func (s *stringList) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		var value string
		if err := node.Decode(&value); err != nil {
			return err
		}
		value = strings.TrimSpace(value)
		if value == "" {
			*s = nil
		} else {
			*s = []string{value}
		}
		return nil
	case yaml.SequenceNode:
		var result []string
		for _, child := range node.Content {
			var item string
			if err := child.Decode(&item); err != nil {
				return err
			}
			item = strings.TrimSpace(item)
			if item != "" {
				result = append(result, item)
			}
		}
		*s = result
		return nil
	default:
		return fmt.Errorf("unsupported YAML type for string list")
	}
}

func (s *stringList) UnmarshalJSON(data []byte) error {
	data = bytesTrimSpace(data)
	if len(data) == 0 {
		*s = nil
		return nil
	}
	if data[0] == '"' {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		value = strings.TrimSpace(value)
		if value == "" {
			*s = nil
		} else {
			*s = []string{value}
		}
		return nil
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	for i := range list {
		list[i] = strings.TrimSpace(list[i])
	}
	*s = list
	return nil
}

func bytesTrimSpace(b []byte) []byte {
	start := 0
	for start < len(b) && (b[start] == ' ' || b[start] == '\n' || b[start] == '\t' || b[start] == '\r') {
		start++
	}
	end := len(b)
	for end > start && (b[end-1] == ' ' || b[end-1] == '\n' || b[end-1] == '\t' || b[end-1] == '\r') {
		end--
	}
	return b[start:end]
}

func loadTransferPlanDocument(path string) (*planDocument, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read plan file: %w", err)
	}
	format := strings.ToLower(filepath.Ext(path))
	if format != ".yaml" && format != ".yml" && format != ".json" {
		format = ".yaml"
	}
	doc, err := decodePlanDocument(data, format)
	if err != nil {
		return nil, err
	}
	if doc.Version == 0 {
		doc.Version = 1
	}
	if doc.Version != 1 {
		return nil, fmt.Errorf("unsupported plan version %d", doc.Version)
	}
	if err := doc.validate(); err != nil {
		return nil, err
	}
	return doc, nil
}

func decodePlanDocument(data []byte, format string) (*planDocument, error) {
	var doc planDocument
	switch format {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("parse plan file: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("parse plan file: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown plan format %q", format)
	}
	return &doc, nil
}

func (doc *planDocument) validate() error {
	for i, ep := range doc.Endpoints.From {
		if strings.TrimSpace(ep.URI) == "" {
			return fmt.Errorf("endpoints.from[%d] missing uri", i)
		}
	}
	for i, ep := range doc.Endpoints.To {
		if strings.TrimSpace(ep.URI) == "" {
			return fmt.Errorf("endpoints.to[%d] missing uri", i)
		}
	}
	return nil
}

func (doc *planDocument) toInputs() ([]endpointInput, []endpointInput, []string, error) {
	fromInputs := make([]endpointInput, 0, len(doc.Endpoints.From))
	for _, ep := range doc.Endpoints.From {
		fromInputs = append(fromInputs, endpointInput{
			Label:          strings.TrimSpace(ep.Name),
			URI:            strings.TrimSpace(ep.URI),
			CredentialHint: strings.TrimSpace(ep.CredentialHint),
			CredentialID:   strings.TrimSpace(ep.CredentialID),
		})
	}

	toInputs := make([]endpointInput, 0, len(doc.Endpoints.To))
	for _, ep := range doc.Endpoints.To {
		toInputs = append(toInputs, endpointInput{
			Label:          strings.TrimSpace(ep.Name),
			URI:            strings.TrimSpace(ep.URI),
			CredentialHint: strings.TrimSpace(ep.CredentialHint),
			CredentialID:   strings.TrimSpace(ep.CredentialID),
		})
	}

	mapSpecs := make([]string, 0, len(doc.Maps))
	for idx, m := range doc.Maps {
		expanded, err := expandPlanMap(idx, m)
		if err != nil {
			return nil, nil, nil, err
		}
		mapSpecs = append(mapSpecs, expanded...)
	}

	return fromInputs, toInputs, mapSpecs, nil
}

func expandPlanMap(index int, m planMap) ([]string, error) {
	fromRefs := m.From
	toRefs := m.To
	if len(fromRefs) == 0 {
		return nil, fmt.Errorf("maps[%d] missing 'from' entries", index)
	}
	if len(toRefs) == 0 {
		return nil, fmt.Errorf("maps[%d] missing 'to' entries", index)
	}

	var specs []string
	for _, fromRef := range fromRefs {
		fromToken := normalizePlanRef(fromRef, "from")
		if m.SourcePath != "" {
			fromToken = fmt.Sprintf("%s:%s", fromToken, m.SourcePath)
		}
		for _, toRef := range toRefs {
			toToken := normalizePlanRef(toRef, "to")
			if m.DestPath != "" {
				toToken = fmt.Sprintf("%s:%s", toToken, m.DestPath)
			}
			tokens := []string{fromToken, toToken}
			if len(m.Options) > 0 {
				keys := make([]string, 0, len(m.Options))
				for k := range m.Options {
					keys = append(keys, strings.ToLower(strings.TrimSpace(k)))
				}
				sort.Strings(keys)
				for _, k := range keys {
					tokens = append(tokens, fmt.Sprintf("%s=%s", k, strings.TrimSpace(m.Options[k])))
				}
			}
			specs = append(specs, strings.Join(tokens, " "))
		}
	}
	return specs, nil
}

func normalizePlanRef(ref, kind string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ref
	}
	if ref == "*" {
		return fmt.Sprintf("%s[*]", kind)
	}
	if strings.HasPrefix(ref, kind+"[") {
		return ref
	}
	return ref
}

func applyPlanParams(target *FileTransferParamsOpts, params *planParams) {
	if params == nil {
		return
	}
	if params.Concurrency != nil {
		target.Concurrency = *params.Concurrency
	}
	if params.Parallelism != nil {
		target.Parallelism = *params.Parallelism
	}
	if params.Pipelining != nil {
		target.Pipelining = *params.Pipelining
	}
	if params.ChunkSize != nil {
		target.ChunkSize = *params.ChunkSize
	}
	if params.RateLimitMbps != nil {
		target.RateLimitMbps = *params.RateLimitMbps
	}
	if params.VerifyChecksum != nil {
		target.VerifyChecksum = *params.VerifyChecksum
	}
	if params.MaxRetries != nil {
		target.MaxRetries = *params.MaxRetries
	}
	if params.RetryBackoffMs != nil {
		target.RetryBackoffMs = *params.RetryBackoffMs
	}
	if params.Overwrite != nil {
		target.Overwrite = *params.Overwrite
	}
	if params.Checksum != nil {
		target.Checksum = *params.Checksum
	}
}
