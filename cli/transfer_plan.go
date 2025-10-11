package cli

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/jgoldverg/grover/backend"
)

type endpointSpec struct {
	Label          string
	URI            string
	CredentialHint string
	CredentialID   string
}

type endpointInput struct {
	Label          string
	URI            string
	CredentialHint string
	CredentialID   string
}

type transferPlan struct {
	Request      *backend.TransferRequest
	sourceSpecs  []endpointSpec
	destSpecs    []endpointSpec
	defaultEdges bool
}

func newTransferPlan(opts *TransferCommandOpts, fromInputs, toInputs []endpointInput, mapSpecs []string) (*transferPlan, error) {
	sources, err := parseEndpointSpecs(fromInputs)
	if err != nil {
		return nil, err
	}
	destinations, err := parseEndpointSpecs(toInputs)
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("at least one --from endpoint is required")
	}
	if len(destinations) == 0 {
		return nil, fmt.Errorf("at least one --to endpoint is required")
	}

	backendSources := make([]backend.Endpoint, len(sources))
	for i, spec := range sources {
		path, err := derivePathFromURI(spec.URI)
		if err != nil {
			return nil, err
		}
		credHint := spec.CredentialHint
		if credHint == "" {
			credHint = strings.TrimSpace(opts.SourceCredID)
		}
		credID := spec.CredentialID
		if credID == "" {
			credID = strings.TrimSpace(opts.SourceCredID)
		}
		backendSources[i] = backend.Endpoint{
			Raw:            spec.URI,
			Scheme:         schemeFromURI(spec.URI),
			Path:           path,
			CredentialHint: credHint,
			CredentialID:   credID,
		}
	}

	backendDestinations := make([]backend.Endpoint, len(destinations))
	for i, spec := range destinations {
		path, err := derivePathFromURI(spec.URI)
		if err != nil {
			return nil, err
		}
		credHint := spec.CredentialHint
		if credHint == "" {
			credHint = strings.TrimSpace(opts.DestCredID)
		}
		credID := spec.CredentialID
		if credID == "" {
			credID = strings.TrimSpace(opts.DestCredID)
		}
		backendDestinations[i] = backend.Endpoint{
			Raw:            spec.URI,
			Scheme:         schemeFromURI(spec.URI),
			Path:           path,
			CredentialHint: credHint,
			CredentialID:   credID,
		}
	}

	edges, err := parseMapSpecs(mapSpecs, sources, destinations)
	if err != nil {
		return nil, err
	}
	plan := &transferPlan{
		sourceSpecs: sources,
		destSpecs:   destinations,
	}
	if len(edges) == 0 {
		plan.defaultEdges = true
		edges = makeDefaultEdges(len(sources), len(destinations))
	}
	plan.sortEdges(edges)
	params, err := opts.TransferParams.toBackend()
	if err != nil {
		return nil, err
	}
	plan.Request = &backend.TransferRequest{
		Sources:        backendSources,
		Destinations:   backendDestinations,
		Edges:          edges,
		Params:         params,
		IdempotencyKey: opts.IdempotencyKey,
	}
	return plan, nil
}

func parseEndpointSpecs(inputs []endpointInput) ([]endpointSpec, error) {
	specs := make([]endpointSpec, 0, len(inputs))
	seenLabels := make(map[string]struct{})
	for _, in := range inputs {
		uri := strings.TrimSpace(in.URI)
		if uri == "" {
			return nil, fmt.Errorf("endpoint definition cannot be empty")
		}
		label := strings.TrimSpace(in.Label)
		if label != "" {
			lower := strings.ToLower(label)
			if _, exists := seenLabels[lower]; exists {
				return nil, fmt.Errorf("duplicate endpoint label %q", label)
			}
			seenLabels[lower] = struct{}{}
		}
		specs = append(specs, endpointSpec{
			Label:          label,
			URI:            uri,
			CredentialHint: strings.TrimSpace(in.CredentialHint),
			CredentialID:   strings.TrimSpace(in.CredentialID),
		})
	}
	return specs, nil
}

func splitLabelAndURI(raw string) (string, string) {
	parts := strings.SplitN(raw, "=", 2)
	if len(parts) == 2 {
		label := strings.TrimSpace(parts[0])
		uri := strings.TrimSpace(parts[1])
		return label, uri
	}
	return "", strings.TrimSpace(raw)
}

func inputsFromRawSpecs(raw []string) ([]endpointInput, error) {
	inputs := make([]endpointInput, 0, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, fmt.Errorf("endpoint definition cannot be empty")
		}
		label, uri := splitLabelAndURI(item)
		if uri == "" {
			return nil, fmt.Errorf("endpoint %q is missing a URI", item)
		}
		inputs = append(inputs, endpointInput{Label: label, URI: uri})
	}
	return inputs, nil
}

var (
	wildcardPattern = regexp.MustCompile(`^(from|to)\[\*\]$`)
	indexPattern    = regexp.MustCompile(`^(from|to)\[(\d+)\]$`)
)

func parseMapSpecs(mapSpecs []string, sources, destinations []endpointSpec) ([]backend.TransferEdge, error) {
	if len(mapSpecs) == 0 {
		return nil, nil
	}
	labelToSource := indexEndpoints(sources)
	labelToDest := indexEndpoints(destinations)
	uriToSource := indexURIs(sources)
	uriToDest := indexURIs(destinations)

	edges := make([]backend.TransferEdge, 0, len(mapSpecs))
	for _, raw := range mapSpecs {
		tokens := strings.Fields(raw)
		if len(tokens) < 2 {
			return nil, fmt.Errorf("invalid --map value %q: expected at least source and destination", raw)
		}
		srcRef, srcOverride := splitEndpointRef(tokens[0])
		dstRef, dstOverride := splitEndpointRef(tokens[1])

		srcIndices, err := resolveEndpointIndices(srcRef, "from", labelToSource, uriToSource, len(sources))
		if err != nil {
			return nil, fmt.Errorf("map %q: %w", raw, err)
		}
		dstIndices, err := resolveEndpointIndices(dstRef, "to", labelToDest, uriToDest, len(destinations))
		if err != nil {
			return nil, fmt.Errorf("map %q: %w", raw, err)
		}
		opts, err := parseMapOptions(tokens[2:])
		if err != nil {
			return nil, fmt.Errorf("map %q: %w", raw, err)
		}

		for _, si := range srcIndices {
			for _, di := range dstIndices {
				edges = append(edges, backend.TransferEdge{
					SourceIndex: si,
					DestIndex:   di,
					SourcePath:  strings.TrimSpace(srcOverride),
					DestPath:    strings.TrimSpace(dstOverride),
					Options:     cloneStringMap(opts),
				})
			}
		}
	}
	return edges, nil
}

func splitEndpointRef(token string) (string, string) {
	token = strings.TrimSpace(token)
	if token == "" {
		return token, ""
	}
	idx := strings.IndexRune(token, ':')
	if idx == -1 {
		return token, ""
	}
	if len(token) > idx+2 && token[idx+1:idx+3] == "//" {
		// URI with scheme://, do not treat as override
		return token, ""
	}
	base := strings.TrimSpace(token[:idx])
	override := strings.TrimSpace(token[idx+1:])
	return base, override
}

func indexEndpoints(items []endpointSpec) map[string]int {
	out := make(map[string]int, len(items))
	for i, item := range items {
		if item.Label == "" {
			continue
		}
		out[strings.ToLower(item.Label)] = i
	}
	return out
}

func indexURIs(items []endpointSpec) map[string]int {
	out := make(map[string]int, len(items))
	for i, item := range items {
		out[item.URI] = i
	}
	return out
}

func resolveEndpointIndices(ref, kind string, labelIndex, uriIndex map[string]int, total int) ([]int, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("empty endpoint reference")
	}
	if wildcardPattern.MatchString(ref) {
		prefix := ref[:strings.IndexRune(ref, '[')]
		if prefix != kind {
			return nil, fmt.Errorf("cannot use wildcard %q for %s endpoints", ref, kind)
		}
		return makeSequentialIndices(total), nil
	}
	if match := indexPattern.FindStringSubmatch(ref); match != nil {
		prefix := match[1]
		if prefix != kind {
			return nil, fmt.Errorf("index reference %q targets wrong endpoint list", ref)
		}
		idx := parsePositiveInt(match[2])
		if idx < 0 || idx >= total {
			return nil, fmt.Errorf("index %d out of range for %s endpoints", idx, kind)
		}
		return []int{idx}, nil
	}
	if idx, ok := labelIndex[strings.ToLower(ref)]; ok {
		return []int{idx}, nil
	}
	if idx, ok := uriIndex[ref]; ok {
		return []int{idx}, nil
	}
	return nil, fmt.Errorf("unknown %s endpoint reference %q", kind, ref)
}

func makeSequentialIndices(total int) []int {
	out := make([]int, total)
	for i := range out {
		out[i] = i
	}
	return out
}

func parsePositiveInt(raw string) int {
	var value int
	for _, r := range raw {
		value = value*10 + int(r-'0')
	}
	return value
}

func parseMapOptions(tokens []string) (map[string]string, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	opts := make(map[string]string, len(tokens))
	for _, raw := range tokens {
		if !strings.Contains(raw, "=") {
			return nil, fmt.Errorf("expected option in key=value form, got %q", raw)
		}
		parts := strings.SplitN(raw, "=", 2)
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key == "" || val == "" {
			return nil, fmt.Errorf("invalid option %q", raw)
		}
		lowerKey := strings.ToLower(key)
		if _, exists := opts[lowerKey]; exists {
			return nil, fmt.Errorf("duplicate option %q", key)
		}
		opts[lowerKey] = val
	}
	return opts, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func makeDefaultEdges(sourceCount, destCount int) []backend.TransferEdge {
	edges := make([]backend.TransferEdge, 0, sourceCount*destCount)
	for si := 0; si < sourceCount; si++ {
		for di := 0; di < destCount; di++ {
			edges = append(edges, backend.TransferEdge{SourceIndex: si, DestIndex: di})
		}
	}
	return edges
}

func (p *transferPlan) usedDefaultEdges() bool {
	return p.defaultEdges
}

func (p *transferPlan) sortEdges(edges []backend.TransferEdge) {
	sort.SliceStable(edges, func(i, j int) bool {
		ei, ej := edges[i], edges[j]
		if ei.SourceIndex != ej.SourceIndex {
			return ei.SourceIndex < ej.SourceIndex
		}
		if ei.DestIndex != ej.DestIndex {
			return ei.DestIndex < ej.DestIndex
		}
		if ei.SourcePath != ej.SourcePath {
			return ei.SourcePath < ej.SourcePath
		}
		if ei.DestPath != ej.DestPath {
			return ei.DestPath < ej.DestPath
		}
		return len(ei.Options) < len(ej.Options)
	})
}

func (p *transferPlan) legacyCompatible() bool {
	if p == nil || p.Request == nil {
		return false
	}
	if len(p.Request.Edges) != 1 {
		return false
	}
	if len(p.Request.Edges[0].Options) > 0 {
		return false
	}
	return true
}

func (p *transferPlan) sourcePath(edge backend.TransferEdge) (string, error) {
	if strings.TrimSpace(edge.SourcePath) != "" {
		return strings.TrimSpace(edge.SourcePath), nil
	}
	if edge.SourceIndex < 0 || edge.SourceIndex >= len(p.Request.Sources) {
		return "", fmt.Errorf("source index %d out of range", edge.SourceIndex)
	}
	return p.Request.Sources[edge.SourceIndex].Path, nil
}

func (p *transferPlan) destPath(edge backend.TransferEdge) (string, error) {
	if strings.TrimSpace(edge.DestPath) != "" {
		return strings.TrimSpace(edge.DestPath), nil
	}
	if edge.DestIndex < 0 || edge.DestIndex >= len(p.Request.Destinations) {
		return "", fmt.Errorf("destination index %d out of range", edge.DestIndex)
	}
	return p.Request.Destinations[edge.DestIndex].Path, nil
}

func derivePathFromURI(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("cannot derive path from empty URI")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return raw, nil
	}
	if u.Path != "" {
		return u.Path, nil
	}
	return raw, nil
}

func schemeFromURI(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err == nil && u.Scheme != "" {
		return strings.ToLower(u.Scheme)
	}
	if idx := strings.Index(raw, "://"); idx > 0 {
		return strings.ToLower(raw[:idx])
	}
	return ""
}

func (p *transferPlan) summaryLines() []string {
	if p == nil || p.Request == nil {
		return nil
	}
	lines := make([]string, 0, len(p.Request.Edges))
	for _, edge := range p.Request.Edges {
		src := p.describeEndpoint(edge.SourceIndex, edge.SourcePath, p.sourceSpecs, p.Request.Sources)
		dst := p.describeEndpoint(edge.DestIndex, edge.DestPath, p.destSpecs, p.Request.Destinations)
		if len(edge.Options) == 0 {
			lines = append(lines, fmt.Sprintf("%s => %s", src, dst))
			continue
		}
		opts := make([]string, 0, len(edge.Options))
		for k, v := range edge.Options {
			opts = append(opts, fmt.Sprintf("%s=%s", k, v))
		}
		sort.Strings(opts)
		lines = append(lines, fmt.Sprintf("%s => %s (%s)", src, dst, strings.Join(opts, ", ")))
	}
	return lines
}

func (p *transferPlan) describeEndpoint(idx int, override string, specs []endpointSpec, eps []backend.Endpoint) string {
	if idx < 0 || idx >= len(specs) {
		return fmt.Sprintf("#%d", idx)
	}
	epSpec := specs[idx]
	display := strings.TrimSpace(override)
	if display == "" {
		if idx >= 0 && idx < len(eps) {
			candidate := strings.TrimSpace(eps[idx].Path)
			if candidate != "" {
				display = candidate
			}
		}
	}
	if display == "" {
		display = epSpec.URI
	}
	if epSpec.Label != "" {
		return fmt.Sprintf("%s (%s)", epSpec.Label, display)
	}
	return display
}
