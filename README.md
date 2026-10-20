# Go CDC + Lua Local MVP

Real MySQL binlog → user Lua transforms → local idempotent JSONL + atomic commit manifest.

Supports fixed tables, ROW/FULL binlog, incremental-only processing, complex stateless Lua, business-key upsert/delete, multiple outputs, persistent checkpoints, pause/resume, and recovery across process restarts.

This delivery is a single-user local MVP. The actual sink is local disk — **not Iceberg, Parquet, or S3**; Iceberg v2 + Parquet remain follow-up goals. Multi-tenant isolation, distributed scheduling, auto scaling, and cloud buffering are not implemented, and this tool is not intended to run untrusted external code.

## Getting Started

Requires Go 1.26+, Docker Compose, curl, and Python 3. All commands are run from the project root.

```bash
make mysql-up
make build
ETL_MYSQL_PASSWORD=etl-demo-password ./bin/etl serve
```

HTTP binds to `127.0.0.1:8080` by default; MySQL runs as a separate Compose container on `127.0.0.1:13306`. The initial account and password are for local demo use only.

In another terminal, create a function, publish a pipeline, and start an instance:

```bash
./scripts/demo.sh
```

Then write data to `shop.orders` and query the output by the returned `instance_id`. Full steps, API semantics, and the scope of this delivery are in the [user guide](docs/user-guide.md).

## Verification

```bash
make test
make e2e
```

`make e2e` uses a real MySQL, the HTTP API, Lua subprocesses, and a temporary disk; it does not start the container automatically, so run `make mysql-up` first. The default `go test ./...` does not run the DB-dependent E2E. See [E2E notes](e2e/README.md).

## Code Structure

| Directory | Content |
| --- | --- |
| `cmd/etl` | serve, validate, and isolated-process Lua worker commands |
| `internal/mysqlcdc` | real replication stream, transaction assembly, schema checks, lossless event encoding |
| `internal/luaruntime` | Lua functions, exact decimal SDK, subprocess execution |
| `internal/localstore` | schema/key validation, idempotent writes, manifest/checkpoint, current-output reads |
| `internal/server` | implemented HTTP API, version and instance lifecycle, persistent control plane |
| `pkg/pipeline` | implemented MVP graph definition, strict JSON parsing, and graph compilation |
| `api` | OpenAPI and JSON Schema for the current MVP |
| `e2e` | Go integration tests |
| remaining `pkg` packages | early interface drafts, not yet wired into the runtime path; not additional supported backends |

The [design](docs/design.md) and [research](docs/research/) keep follow-up architecture discussions; the implemented scope of this delivery is authoritative in the [user guide](docs/user-guide.md) and the OpenAPI. Known defects and future plans are in the [roadmap](docs/roadmap.md).
