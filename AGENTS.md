# AGENTS.md

Project guide for AI coding assistants and collaborators. Read this file before changing code; keep it in sync with the project when you change what it describes.

## One-liner

A single-user local ETL tool: MySQL binlog CDC → user-defined Lua transforms → local idempotent JSONL sink with atomic commit manifests (Go 1.26+). Currently an MVP; the sink is not Iceberg/Parquet/S3.

## Build and verify

```bash
make mysql-up    # start MySQL 8.4 container (127.0.0.1:13306, e2e prerequisite)
make build       # build bin/etl
make test        # go test ./... (excludes DB-dependent e2e)
make lint        # full golangci-lint check (requires golangci-lint)
make e2e         # integration tests with real MySQL + HTTP API + Lua subprocess; run mysql-up first
```

Before committing: `make lint` must report zero issues and `make test` must be green; run `make e2e` when touching CDC / idempotency / lifecycle logic.

## Directory map

| Path | Content |
| --- | --- |
| `cmd/etl` | serve / validate / isolated Lua worker subprocess commands |
| `internal/mysqlcdc` | real replication stream, transaction assembly, schema checks |
| `internal/luaruntime` | Lua functions, exact-decimal SDK, subprocess execution (time-boxed isolation, not a resource sandbox) |
| `internal/localstore` | idempotent writes, manifest/checkpoint, current-output reads |
| `internal/server` | HTTP API, version and instance lifecycle, persistent control plane |
| `internal/model`, `pkg/cdc` | two CDC models coexist (inconsistent field types, see docs/roadmap.md K20); confirm which one a change targets |
| remaining `pkg/` | interface drafts not wired into the runtime path; do not assume they take effect when modifying the runtime |
| `api/` | OpenAPI and JSON Schema, generated from `scripts/generate_api.py` as the single source of truth; change the script, then regenerate |
| `docs/` | design, user guide, research, roadmap |
| `e2e/` | Go integration tests |

## Hard rules

1. **Docs honesty first**: new features must update `docs/user-guide.md` and the OpenAPI contract; capabilities that are not implemented must be explicitly labeled "not implemented" — never over-promise. This is the project's core discipline.
2. **Three-way contract sync**: HTTP behavior ↔ `internal/server` ↔ `api/openapi.json` must stay consistent; after API changes run `scripts/generate_api.py` and diff `api/` to confirm zero drift.
3. **The idempotency chain is untouchable**: transaction-ID content addressing + atomic manifest publish + file checksums are the core correctness guarantee. No change may introduce a partially-visible intermediate state; the checkpoint may only advance after a successful commit.
4. **Lua isolation boundary**: the worker is a separate subprocess + 2s time box + empty environment — **not a security sandbox** (no memory limits). Do not add requirements that need resource isolation for untrusted code execution unless RLIMIT/cgroup support lands in the same change (see roadmap K4).
5. **Lock granularity**: the global lock in `internal/server` must only protect short catalog critical sections; never add network I/O or subprocess calls while holding it (the existing problem is roadmap K1 — do not regress it).
6. **English everywhere**: docs, README, code, commit messages, and API error text are all in English.

## Common pitfalls

- e2e does not start the container itself: run `make mysql-up` before `make e2e`, or DB-dependent tests are silently skipped.
- MySQL prerequisites: source must have ROW/FULL binlog, binlogs must not have expired, and `binlog_transaction_compression` must be off (a `TRANSACTION_PAYLOAD_EVENT` stops the instance, see roadmap K7).
- Error handling: internal errors currently return 422 and leak detail (known defect K5); do not extend this pattern in new code — internal errors should use 500/503.
- Idempotency conflicts (`IDEMPOTENT_CONFLICT` / `SEQUENCE_GAP` / `CONTENT_CONFLICT`) are terminal semantics; do not paper over them with silent retries.
- `catalog.json` (control plane) and the store manifest (data plane) are two separate persistence truths; crash recovery relies on each side's idempotency. When changing either write path, evaluate against roadmap K18.

## Planning reference

Known defects and capability gaps are maintained in [docs/roadmap.md](docs/roadmap.md). Check it before starting a task: note progress on the corresponding entry when fixing a bug, and add an entry when introducing a new limitation (e.g. a capacity cap).
