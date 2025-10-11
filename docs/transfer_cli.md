# Transfer CLI Plans

Grover's `transfer cp` command supports two ways to describe scatter/gather workflows:

1. Command-line flags (`--from`, `--to`, `--map`).
2. A reusable plan file referenced with `--plan`. Plan files make it easy to commit complex transfer graphs or share them between teams.

This document describes the plan file schema, how it maps onto the CLI flags, and provides example usage.

## Plan File Format

Plans are plain YAML or JSON documents. YAML is assumed when no extension is provided. The top-level fields are:

| Field            | Type                 | Description |
|------------------|----------------------|-------------|
| `version`        | integer              | Schema version. Only `1` is currently supported. If omitted, defaults to `1`. |
| `via`            | string               | Optional override for the execution route (`auto`, `client`, `server`). Equivalent to `--via`. |
| `idempotency_key` | string              | Optional explicit idempotency key that will be passed to the transfer request. |
| `params`         | object               | Optional transfer tuning knobs (same fields as the CLI flags). |
| `endpoints.from` | list of endpoints    | All source endpoints used in the plan. |
| `endpoints.to`   | list of endpoints    | All destination endpoints used in the plan. |
| `maps`           | list of map objects  | Mapping rules describing the edges between sources and destinations. |

### Endpoint Object

```yaml
name: build               # optional label; referenced by maps
uri:  file:///builds/artifact.tar
credential_id: build-prod # optional credential profile to use for this endpoint
credential_hint: default  # optional hint used during resolution
```

Labels are optional but highly recommended when a plan references the same URI multiple times. The CLI will also accept index references (`from[0]`) or wildcards (`from[*]`) in map entries. If `credential_id` (or `credential_hint`) is omitted the CLI falls back to the global `--source-credential-id` / `--dest-credential-id` flags when building the request.

### Map Object

```yaml
from: build             # string or list of strings
# or: [build, logs]

to: [ops, archive]      # string or list of strings
source_path: "**/*.tar" # optional per-map override
Dest_path:   "/reports/"
options:
  overwrite: if-different
  checksum: sha256
```

`from` and `to` accept either a single string or an array of strings. The following references are valid:

- A label defined in `endpoints.from`/`endpoints.to`.
- `*` — expands to all endpoints on that side (e.g. `from: "*"`).
- Indexed references (`from[0]`, `to[1]`).

`source_path` and `dest_path` correspond to the positional `source:/override` and `dest:/override` portions of the CLI `--map` flag.

`options` is a free-form map of `key: value`; today the CLI simply passes the pairs through ready for future per-edge policies. Keys are normalised to lowercase when expanded into CLI map strings.

If no `maps` list is supplied, the CLI automatically generates the cartesian product of `from` and `to` endpoints (all sources -> all destinations).

## Example Plan

```yaml
version: 1
via: client
params:
  concurrency: 8
  overwrite: if-different
  verify_checksum: true

endpoints:
  from:
    - name: build
      uri: file:///builds/report.pdf
      credential_id: local-files
    - name: logs
      uri: file:///var/logs/**/*.gz
  to:
    - name: ops
      uri: sftp://ops@reports/incoming/
      credential_id: ops-prod
    - name: backup
      uri: grover://cold-storage/reports/
      credential_id: grover-archive

maps:
  - from: build
    to: [ops, backup]
    dest_path: /reports/report.pdf
    options:
      overwrite: always
  - from: logs
    to: ops
    dest_path: /logs/
```

Running the plan:

```bash
grover transfer cp --plan path/to/plan.yaml
```

You can still append ad-hoc overrides at the command line.

```bash
grover transfer cp --plan plan.yaml \
  --from adhoc=file:///tmp/supplement.csv \
  --map adhoc ops:/reports/supplement.csv overwrite=always
```

## Validation

The CLI validates plan files before executing:

- Checks that every endpoint URI is non-empty.
- Ensures at least one source and destination exist after combining plan + flags.
- Verifies that each map entry contains both `from` and `to` references.
- Normalises wildcard (`*`) references into `from[*]` / `to[*]` for compatibility with the planner.

If validation fails, the command exits early with an error pointing to the offending section (for example `maps[1] missing 'to' entries`).

A dedicated `grover transfer plan validate` command is not yet implemented, but the `--plan` option may be run with `--dry-run` in future iterations to preview complex jobs.

## Relationship to CLI Flags

Plan file entries expand to the same internal representation as their CLI counterparts:

- Each endpoint becomes a `--from` or `--to` entry (label + URI).
- Each map expands into the positional `--map "source dest [option=value]"` string.
- `params` apply the same flags (`--concurrency`, `--overwrite`, etc.).
- `via` and `idempotency_key` override the matching CLI flags if provided.

Because both pathways converge on the same planner, command-line overrides merge naturally with plan-defined values.

## Future Enhancements

- Explicit validation command (`grover transfer plan validate`).
- Richer map options (per-edge retry policies, checksum requirements, delete-after-transfer flags).
- Named plan registries under `~/.grover/plans/` for quick reuse (`--plan build-release`).

For now, plan files provide a concise, reproducible way to describe complex transfer graphs without abandoning the quick inline `--from/--to/--map` experience.
