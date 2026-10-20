# Roadmap and Known Limitations

This document records the current MVP's **known defects** (problems that exist in the code and await fixes) and **missing capabilities** (gaps relative to a full CDC/ETL product), plus their priority order. The implemented scope is authoritative in the [user guide](user-guide.md) and the OpenAPI contract; this document is updated as the code evolves — see the verification date at the end.

## Known Defects

### Critical

| # | Issue | Location | Notes and plan |
| --- | --- | --- | --- |
| K1 | Heavy I/O under a global mutex | `internal/server/http.go:76` | `mutate()` runs MySQL Probe (up to 10s) and the Lua worker subprocess (up to 2s) while holding the lock, queuing all read APIs. Plan: keep only "validate + register" in a short critical section, move heavy work outside the lock, and register results after a second validation. |
| K2 | CDC has zero reconnects, and errors pin the instance permanently | `internal/mysqlcdc/mysql.go:187`, `internal/server/server.go:241` | `DisableRetrySync` + pause-on-error rewrites DesiredState and persists it, so the instance does not even recover after a process restart. Plan: add exponential-backoff reconnects (distinguishing retryable network errors from non-retryable schema/permission errors); pausing must not rewrite DesiredState. |
| K3 | SEQUENCE_GAP / CONTENT_CONFLICT have no recovery path | `internal/localstore/store.go:214` | Any catalog/manifest inconsistency permanently wedges the instance; the only escape is deleting the data directory. Plan: provide a repair/resync operator interface that truncates to a given sequence and recommits. |
| K4 | Lua isolation is a time box, not a resource sandbox | `internal/luaruntime/runtime.go:24` | The 2s timeout limits time but not memory; a malicious or runaway script can exhaust host memory before the deadline. The empty environment only blocks credential inheritance. Plan: add `RLIMIT_AS` or a dedicated cgroup to the worker; docs already state this is not a security boundary. |
| K5 | Internal errors always return 422 and leak internals | `internal/server/http.go:31` | MySQL disconnects and disk I/O errors are wrapped as "invalid client request", so clients cannot retry appropriately, and raw internal error text is exposed. Plan: add 500/503 branches and sanitize error details. |
| K6 | In-memory catalog "rollback" via JSON round-trip swallows errors and cannot undo side effects | `internal/server/http.go:107` | The `json.Marshal` error is ignored; side effects already executed by `fn` (worker subprocess ran, directories created) cannot roll back with the in-memory state. Plan: separate validation from mutation — compute all side effects first, then commit under a single lock acquisition. |

### Medium

| # | Issue | Location | Notes and plan |
| --- | --- | --- | --- |
| K7 | Sources with `binlog_transaction_compression` unsupported | `internal/mysqlcdc/mysql.go:213` | MySQL 8.0.20+ with transaction compression hits `TRANSACTION_PAYLOAD_EVENT` and stops; Probe does not detect the setting either. Plan: detect the setting during Probe and list it in the documented prerequisites. |
| K8 | Replication server ID allocation assumes instances are never deleted | `internal/server/http.go:353` | `ServerID + len(Instances)` can hand out an ID already in use after a catalog rollback or manual edit; two binlog syncers sharing one ID kick each other off, with bizarre symptoms. Plan: allocate max(used IDs) + 1 from the catalog. |
| K9 | Output view replays full history and shares one store lock with commits | `internal/localstore/store.go:336` | `Snapshot` holds the lock while re-reading and re-decoding all data files and replaying upsert/delete, blocking worker `Commit`; once history grows, one GET can stall the data plane. Plan: pagination / incremental snapshots / read-write lock. |
| K10 | The 64 MiB manifest cap is the instance lifetime cap | `internal/localstore/store.go:25` | The manifest is fully rewritten on every commit and capped at 64 MiB; reaching the cap permanently fails the instance — no compaction, archival, or truncation. Plan: segment the manifest or switch to an append log, and expose a watermark in capabilities. |
| K11 | TIMESTAMP semantics have no end-to-end test | `e2e/mysql_lua_test.go:52` | A `timestampTable` is created but never inserted into or asserted against; the user guide §6 claims about UTC semantics (TIMESTAMP as UTC, DATETIME without timezone) have zero test backing. Plan: add e2e assertions. |
| K12 | The pause/resume state machine leans on string states + mutex serialization | `internal/server/server.go:216` | Cleanup from an old worker goroutine can overwrite state after a resume; no test guards this race window (e2e only covers sequential pause→resume). Plan: introduce generation/epoch validation. |
| K13 | `Validate` hardcodes `context.Background()` + 1s | `internal/luaruntime/runtime.go:99` | The cancellable ctx passed from the HTTP layer is discarded; init cannot be cancelled on client disconnect, the 1s deadline caps initialization complexity, and neither is configurable. |

### Low priority

| # | Issue | Location | Notes |
| --- | --- | --- | --- |
| K14 | Batch loop is O(n²) marshal and swallows errors | `internal/server/server.go:292` | Re-marshals the full candidate every iteration; should incrementally estimate or marshal once then slice. |
| K15 | Every event rebuilds the VM and recompiles the whole Lua source | `internal/luaruntime/runtime.go:131` | A 10k-event transaction means 10k compilations of the same source; parsing the chunk once (while keeping the per-event fresh-globals semantics) is an order-of-magnitude win. |
| K16 | `Server.Close` leaks store/lock on the timeout path | `internal/server/server.go:159` | The `ctx.Done()` branch returns without `store.Close()` and file-lock release. Harmless on process exit, but leaves dirty lock files in tests/embedded use. |
| K17 | `string.dump` not removed from the Lua string library | `internal/luaruntime/runtime.go:45` | Whitelist completeness gap; negligible impact in this scenario. |
| K18 | catalog and store manifest are two separate persistence truths | `internal/server/server.go:205`, `internal/localstore/store.go:267` | Instance metadata/catalog idempotency keys live in the catalog; checkpoints live in the manifest; crash recovery is stitched together from each side's idempotency. It holds today, but K6/K8 fragility stems from this split. Plan: draw a crash matrix in the design doc and add tests for it. |
| K19 | main's exit path handles errors loosely | `cmd/etl/main.go:106` | The `Shutdown` error is ignored and the first error source is lost. |
| K20 | Two CDC models with inconsistent field types | `internal/model/model.go:8`, `pkg/cdc/types.go:13` | Position JSON tags and widths (uint32 vs uint64) disagree — a type landmine when GTID/switchover support lands. Unify now. |

## Missing Capabilities

Relative to a production CDC/ETL product, the MVP deliberately skips the following, planned by priority:

### P0 — from interview assignment to "demoable to others"

| Capability | Current state | Plan |
| --- | --- | --- |
| CI | **delivered**: `.github/workflows/ci.yml` runs golangci-lint, gofmt/vet, `go test -race`, and e2e against a real MySQL container on every push and PR | — |
| Failure-injection e2e | only graceful `Close()` restart; no SIGKILL/worker-kill/disconnect scenarios | kill -9 mid-crash recovery, kill Lua worker mid-execution, source disconnect/reconnect |
| Fate of the `pkg/` draft packages | `buffer`/`cdc`/`checkpoint`/`listener`/`orchestrator`/`sink`/`transform` have zero tests and are not wired in | wire them in or demote to docs attachments to avoid a "dead code" impression |
| README architecture diagram | **delivered**: implemented-path mermaid diagram in [README](../README.md) | — |
| LICENSE | missing | add MIT/Apache-2.0 |

### P1 — from MVP to a credible single-node tool

| Capability | Current state | Plan |
| --- | --- | --- |
| CDC reconnect | zero reconnects (see K2) | exponential backoff + heartbeat + error classification |
| Operator recovery path | consistency errors only fixable by deleting data (see K3) | repair/resync interface |
| API error semantics | internal errors always 422 (see K5) | 500/503 branches + sanitization |
| Output read scalability | full-history replay (see K9) | pagination, incremental snapshots |
| Manifest size | dies at 64 MiB (see K10) | segmented manifest / append log + watermark exposure |
| Resource isolation | time-box sandbox (see K4) | RLIMIT/cgroup |
| Lua performance | per-event recompile (see K15) | chunk reuse |
| Large transactions | fixed 100-event / 900KB batch-split thresholds | configurable + large-transaction split tests |

### P2 — research-stage directional goals (see [design.md](design.md) and [research/](research/))

- **Clustered listener and transforms (design settled 2026-10-20, not scheduled)**: the per-instance single-reader model stays — one leader reader per instance is deliberate and matches Debezium/Maxwell, since a single MySQL binlog stream cannot be read in parallel. Scale-out targets the transform layer instead: `hash(business_key) % N` shard routing between reader and workers (per-key order preserved, global order intentionally dropped), stateless Lua workers as the scaling unit, a transaction commit barrier plus per-shard watermarks as the correctness core, and DDL as a global pause barrier. Reader HA adds leader election + fencing + a shared checkpoint store; the existing idempotency chain already absorbs the at-least-once replay this introduces. Reference points: Debezium (single reader + Kafka key partitioning), GoldenGate parallel Replicat (dependency-aware apply), TiCDC (key-range parallel pull — only possible for distributed storage sources, not MySQL), Flink CDC (PK-chunk parallel snapshot for the future initial-load goal).
- **Iceberg v2 + Parquet / S3 sink**: the sink today is local JSONL only, as explicitly stated in the docs
- **GTID support**: currently binlog file + position only (`internal/model`); switchover scenarios need GTID — also a prerequisite for clustered-reader failover
- **Multi-tenant isolation, autoscaling, cloud buffering**
- **WASM as a Lua-subprocess replacement**: discussed in the design doc, for stronger isolation and lower process overhead
- **Throughput/latency benchmarks**: no benchmark data at all

## Status

- Initial version: 2026-10-19, based on a full code review (including an actual `go test ./...` run)
- 2026-10-20: marked CI and README architecture diagram delivered (P0); recorded the settled clustering/sharding design (P2)
- Next verification: after each feature delivery, update the corresponding entry status
