# Local CDC + Lua MVP User Guide

This document describes the **actual implementation** of this release, not a future product commitment. The API version is `v1`; the source is real MySQL, the sink is local JSONL/manifest, and Lua uses GopherLua's Lua 5.1 language model.

## 1. Scope of This Release

| Capability | Support and limits in this release |
| --- | --- |
| Source | One preconfigured `mysql-local` connection; each pipeline explicitly subscribes to 1–16 fixed InnoDB tables |
| Binlog | Binlog must be enabled with `ROW` + `FULL`; normal committed transactions only, file/position recovery, no GTID failover |
| Initial data | Incremental only; the current position is captured at instance creation, and existing data is not scanned |
| Pipeline graph | Exactly one MySQL source → one Lua transform → 1–16 local sinks; arbitrary chaining, joins, and cycles are not accepted |
| User logic | Lua conditions, loops, functions, string handling, and same-event computation; can filter, fan out one-to-many, and route to declared outputs |
| Output model | `upsert` / `delete` with explicit business keys and a fixed schema; multiple changes returned in one call are applied in array order |
| Storage | Local uncompressed JSONL change files and an atomic manifest; HTTP serves key-materialized current results |
| Reliability | All logical outputs and the checkpoint are committed in a single manifest; rewriting identical content for the identical transaction does not create duplicate data files |
| Lifecycle | Immutable functions, editable drafts, immutable versions, one immutable-version instance per pipeline; pause/resume, and recovery on service restart in the same data directory |
| DDL | DDL on subscribed tables/schemas pauses; unrelated table/database DDL that can be parsed safely advances as a zero-output operation; unknown statements still pause |
| Not implemented | Iceberg/Parquet/S3, cloud backlog archival, initial full load, historical recomputation, GTID failover, automatic schema evolution, window state, external APIs, distributed/multi-tenant operation, and auto-scaling |

The local service only listens on numeric loopback addresses and has no account authentication, production-grade tenant isolation, or a hard memory sandbox. The Lua subprocess does not inherit platform credentials, has restricted library capability, and has an execution timeout, but this **does not constitute a production security boundary for running untrusted external code**. Do not expose the service to the public internet or shared networks through a proxy. The data directory should be owned exclusively by the current user and must not be modified by other users or programs.

## 2. Starting Real MySQL and the Service

Dependencies: Go 1.26+, Docker Desktop/Engine, Docker Compose, curl, Python 3; macOS/Linux supported (local file locking uses flock).

```bash
make mysql-up
make build
ETL_MYSQL_PASSWORD=etl-demo-password ./bin/etl serve \
  -listen 127.0.0.1:8080 \
  -data .local/etl \
  -mysql-addr 127.0.0.1:13306 \
  -mysql-user etl
```

Compose creates a dedicated MySQL 8.4 container, the `shop.orders` table, and an `etl` account with SELECT/replication privileges only. MySQL binds to local port 13306; the root demo password is `etl-demo-root` and the replication account password is `etl-demo-password`. Credentials are local development configuration and are never written into the Pipeline or Lua.

Query the running capabilities and the source through the service:

```bash
curl -sS http://127.0.0.1:8080/healthz
curl -sS http://127.0.0.1:8080/v1/capabilities
curl -sS 'http://127.0.0.1:8080/v1/connections/mysql-local/inspect?tables=shop.orders'
```

Source columns currently support integers, DECIMAL, UTF-8/ASCII strings, binary/blob, JSON, and date/time/datetime/timestamp/year; FLOAT/DOUBLE, BIT, ENUM/SET, spatial types, generated columns, invisible columns, and other character sets are rejected to avoid implicit corruption. The target `float64` type may be produced by Lua computation; this does not mean source floating-point columns are supported.

inspect returns the current file/position, the server UUID, and column information, and also checks ROW/FULL, InnoDB, and column types. It does not create a consumption task and does not advance the checkpoint. Connections are only configured via service flags; the MVP has no connection management or secret management API.

## 3. Functions, Drafts, Versions, and Instances

- **Function**: an immutable resource of Lua source code; any content change requires registering a new function, and existing functions are never overwritten.
- **Pipeline Draft**: an editable graph definition that uses revision and ETag to handle concurrent edits.
- **Pipeline Version**: a fixed snapshot of a published draft, containing function references, output schema, and graph configuration.
- **Instance**: a continuously consuming runtime instance pinned to one version, source identity, schema baseline, and initial starting position. Restarting does not create a new instance or reset progress.

The MVP creates only one instance per pipeline and does not support stop-and-delete or online upgrade. Editing a draft or publishing a new version does not update existing instances. Switching to a new version that fixes code/schema while preserving the original consumption position is not yet provided; when you hit such an error, preserve the data directory and binlogs first, and do not fake lossless recovery by recreating the instance. A new pipeline that starts from the current position will miss previously unprocessed history; you must explicitly accept this boundary.

## 4. End-to-end Quickstart

Create the function, the draft, publish a version, and start an instance:

```bash
./scripts/demo.sh
```

The script reads `examples/order-transform.lua` and `examples/pipeline.json` and replaces the function reference; requests and responses are saved under `.local/demo/`, and the instance response contains `instance_id`.

```bash
INSTANCE_ID=$(python3 -c 'import json; print(json.load(open(".local/demo/instance.json"))["instance_id"])')
curl -sS "http://127.0.0.1:8080/v1/instances/$INSTANCE_ID"
```

Wait until the actual status is `RUNNING` (this only means the worker has started; final success is determined by the checkpoint and outputs), then write real transactions to the dedicated container:

```bash
docker compose exec -T mysql mysql -uroot -petl-demo-root -e \
  "INSERT INTO shop.orders VALUES (1001, 7, 12.34, 'paid');"
curl -sS "http://127.0.0.1:8080/v1/instances/$INSTANCE_ID/outputs/orders"
```

After asynchronous processing completes, the result contains (the checkpoint ID and position are illustrative):

```json
{
  "rows":[{"order_id":"1001","customer_id":"7","total":"24.68"}],
  "checkpoint":{
    "sequence":1,
    "transaction_id":"source-transaction-id",
    "version_id":"ver_example",
    "position":{"file":"mysql-bin.000003","position":25813}
  }
}
```

The example only outputs paid orders, with the amount multiplied by 2 using exact decimal. Then verify updates and deletes respectively (poll the output after each statement):

```bash
docker compose exec -T mysql mysql -uroot -petl-demo-root -e \
  "UPDATE shop.orders SET amount=15.55 WHERE id=1001;"
docker compose exec -T mysql mysql -uroot -petl-demo-root -e \
  "DELETE FROM shop.orders WHERE id=1001;"
```

After the update, `total` is the string `"31.1"`, and after the delete that business key disappears. The exact decimal SDK returns normalized strings and does not guarantee trailing zero padding; `31.1` is compatible with decimal(18,2).

The script's idempotency keys are fixed; re-running the same script returns the original resources, does not create a second task, and does not automatically resume paused instances. Do not modify the request content and reuse an old key. When a demo ID already exists, use a different SQL primary key instead; do not wipe data automatically for the demo.

## 5. Pipeline Definition

The complete example is `examples/pipeline.json`; the structure is as follows:

```json
{
  "api_version":"v1",
  "kind":"Pipeline",
  "metadata":{"name":"orders-export"},
  "spec":{
    "nodes":[
      {"id":"mysql","type":"source.mysql","mysql":{
        "connection_ref":"mysql-local","tables":["shop.orders"],"start":{"mode":"current"}
      }},
      {"id":"normalize","type":"transform.lua","lua":{
        "function_ref":"fn_RETURNED_BY_API","timeout_ms":2000
      }},
      {"id":"archive","type":"sink.local","local":{
        "output":"orders","key_fields":["order_id"],
        "fields":[
          {"name":"order_id","type":"string","required":true},
          {"name":"customer_id","type":"string","required":true},
          {"name":"total","type":"decimal(18,2)","required":true}
        ]
      }}
    ],
    "edges":[{"from":"mysql","to":"normalize"},{"from":"normalize","to":"archive"}]
  }
}
```

- `start.mode=current` is resolved only once, at instance creation. Pause/restart both resume from the committed position; you cannot switch to latest.
- Node, field, and logical output names start with a lowercase letter, are at most 63 characters, and may contain digits, `_`, and `-`. Fixed tables are simple `database.table` identifiers; escaped names, wildcards, and dynamic additions are not supported.
- Each sink's `output` name is unique; Lua may only write to these names and cannot pick arbitrary file paths or create tables.
- All sinks are mandatory branches. Even if an output has no changes in a given transaction, an explicit empty output is still written and participates in the unified commit.
- `timeout_ms` is 1–5000 and constrains each invocation batch; the Lua module additionally has a 2-second execution cap, and the smaller of the two takes effect. A single event does not get to monopolize all of the time.
- `key_fields` must be required, non-floating-point fields in the schema; when a business key changes, Lua must emit an old-key delete and a new-key upsert.
- `fields` supports only `string`, `bool`, `int64`, `uint64`, `float64`, and `decimal(p,s)` (p≤38, 0≤s≤p). Unknown fields, type errors, and incomplete required fields fail the transaction. Optional fields may be missing or null; upsert replaces the whole row, and omitted optional fields do not retain old values.

The CLI can validate the graph: `./bin/etl validate -file .local/demo/pipeline.json`. The CLI performs only structural and graph validation; it does not check function existence or establish a database connection; HTTP validate additionally checks function references. The API accepts JSON only; a general YAML parser is not implemented. The JSON Schema is for editors and clients; the service uses a Go validator with the same semantics and does not fetch arbitrary remote schema references.

## 6. Lua Programming Protocol

### Input

`function transform(event)` receives one complete CDC row change. A new VM is created per event, so global variables cannot accumulate state across events.

```json
{
  "event_id":"stable-source-event-id",
  "source_id":"mysql-server-uuid",
  "transaction_id":"stable-source-transaction-id",
  "row_index":0,
  "database":"shop",
  "table":"orders",
  "op":"update",
  "before":{"id":"1001","customer_id":"7","amount":"12.34","status":"paid"},
  "after":{"id":"1001","customer_id":"7","amount":"15.55","status":"paid"},
  "columns":[{"name":"id","type":"bigint unsigned","required":true}],
  "source_position":{"file":"mysql-bin.000001","position":1234}
}
```

`columns` actually contains all columns of the table. For an insert, `before`, and for a delete, `after`, are `etl.null`; an in-row SQL NULL also uses this sentinel and is not Lua nil. `etl.null` is truthy in Lua conditions, so you cannot pick a mirror via `event.after or event.before` — branch explicitly on `event.op` instead; test for a null field with `field == etl.null`. `event_id` is bound to the binlog position and stays stable when the same source replays; reuse across source rebuilds or after a binlog reset is not guaranteed.

Integers/DECIMAL are passed as strings to avoid Lua numbers losing precision for large integers. Binary is Base64; MySQL JSON is the raw JSON text string and is not automatically converted into a table; times are source-semantic strings, with TIMESTAMP in UTC and DATETIME carrying no added timezone. There is no built-in JSON decoding SDK yet. See the source column types returned by inspect for details.

### Output

Return a contiguous Lua array, each item being:

```lua
{output="orders", op="upsert", key={order_id="1001"},
 value={order_id="1001", customer_id="7", total="24.68"}}
```

or a delete:

```lua
{output="orders", op="delete", key={order_id="1001"}}
```

`return {}` means filter out; `return nil`, sparse arrays, mixed arrays, circular references, non-finite numbers, and function values are all errors. A delete must omit `value`; an upsert must include the complete `value`; `key` must contain exactly the `key_fields` and match the corresponding fields in `value`. The output op does not have to match the source op.

One-to-many is done by returning multiple mutations; multiple outputs are routed by different output names. Stable child keys, deleting old child items, and retracting emitted data after a state-filter exit are all implemented by the user based on before/after. See the paid→cancelled delete and primary-key-change handling in `examples/order-transform.lua`. The platform does not automatically infer from an empty returned array that old data should be deleted.

The platform adds `mutation_id`, `source_event_id`, and `output_index` to each mutation; do not set these fields yourself. User functions must return the same order and content for the same input; `pairs` iteration order over objects is not a stable protocol — sort explicitly before generating arrays; replay content conflicts stop the pipeline and cannot overwrite already-written different content.

### SDK and Limits

The available whitelist covers basic functions plus string/table/math (without random/randomseed), and:

| API | Behavior |
| --- | --- |
| `etl.null` | JSON/SQL null sentinel, preserves null values in objects |
| `etl.decimal_add(a,b)` | Exactly adds two decimal strings, returns a string |
| `etl.decimal_mul(a,b)` | Exactly multiplies two decimal strings, returns a string |

Decimal operands are at most 128 bytes and only plain decimal format is accepted. Do not use `tonumber` for amounts and business IDs. There is no os/io/package/debug/print/dofile/loadfile/loadstring, and no network or external access. Function registration only compiles, initializes, and validates the transform entry; real input and output schema are validated at runtime.

The source code limit is 64 KiB; worker input is 1 MiB and output 4 MiB. The executor splits batches by at most 100 events and roughly 900 KB of input; a single event that cannot fit pauses the instance. A transaction is committed only after all its batches succeed. The subprocess does not inherit the MySQL password and is killed on timeout; there is no reliable hard memory limit yet — do not accept malicious code.

## 7. HTTP API

Successful responses are `application/json`; errors are RFC 9457-style `application/problem+json`. Write requests must have `Content-Type: application/json`; the request limit is 1 MiB, and unknown fields, duplicate JSON keys, and multi-document payloads are rejected. Browser write requests carrying an Origin header are rejected; the MVP targets curl/SDK usage.

All write requests except validate require an `Idempotency-Key` (1–128 characters). The original request and response are stored keyed by method, path, and key: retries with the same body and If-Match return the original result without re-execution; reusing a key with a different body or precondition returns 409. Records persist in the local catalog and the MVP does not expire them automatically, so watch catalog growth. Equivalent JSON with different byte content is also treated as a different request. Only a new key initiates a new operation.

| Method and path | Request/behavior |
| --- | --- |
| `GET /healthz` | Process liveness; does not indicate MySQL availability or pipeline health |
| `GET /v1/capabilities` | Capabilities of this implementation |
| `GET /v1/connections/mysql-local/inspect?tables=shop.orders` | Source configuration and type check, current position; does not create a task |
| `POST /v1/functions` | `{ "name":"orders", "source":"function transform(event) ... end" }` → 201 Function |
| `GET /v1/functions/{function_id}` | Immutable source and identity |
| `POST /v1/pipelines/validate` | Full Pipeline document → 200 compiled plan, not persisted |
| `POST /v1/pipelines` | Full Pipeline document → 201 draft with ETag |
| `GET /v1/pipelines` | `{ "items": [...] }`; no pagination in MVP |
| `GET /v1/pipelines/{pipeline_id}` | Draft with ETag |
| `PUT /v1/pipelines/{pipeline_id}` | Fully replaces the draft, requires If-Match, returns the new revision |
| `POST /v1/pipelines/{pipeline_id}/versions` | `{ "draft_revision":"1" }` → 201 immutable Version |
| `GET /v1/pipelines/{pipeline_id}/versions/{version_id}` | Published version and compiled plan |
| `POST /v1/pipelines/{pipeline_id}/instances` | `{ "version_id":"ver_..." }` → 201 STARTING, captures the current starting position and schema |
| `GET /v1/instances/{instance_id}` | actual/desired status, starting position, schema, error summary |
| `POST /v1/instances/{instance_id}/pause` | `{}` → 202, asynchronously cancels in-flight work and preserves a safe checkpoint |
| `POST /v1/instances/{instance_id}/resume` | `{}` → 202, starts from the existing checkpoint |
| `GET /v1/instances/{instance_id}/checkpoint` | Committed sequence, transaction ID, version, file/position; initial sequence=0 |
| `GET /v1/instances/{instance_id}/outputs/{output}` | `{ "rows": [...], "checkpoint": {...} }`, data and progress from the same commit snapshot, no pagination |

The first instance request needs no user-uploaded schema_baseline_ref: the service collects and pins the baseline from the current source. Subsequent recovery rejects source server UUID changes and schema changes; it does not automatically re-capture the starting position or backfill data for existing tables.

Example of creating a function:

```bash
python3 - <<'PY' > .local/function.json
import json
from pathlib import Path
print(json.dumps({"name":"order-transform","source":Path("examples/order-transform.lua").read_text()}))
PY
curl --fail-with-body -sS http://127.0.0.1:8080/v1/functions \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: my-function-1' \
  --data-binary @.local/function.json
```

Error example:

```json
{
  "type":"urn:etl:problem:revision_conflict",
  "title":"Precondition Failed",
  "status":412,
  "code":"REVISION_CONFLICT",
  "detail":"draft changed"
}
```

Common status codes: 400 request/idempotency-key errors, 404 resource not found, 409 name/state/idempotency conflict, 412 draft version mismatch, 413 request too large, 415 wrong Content-Type, 422 structural/capability/function/schema errors, 428 missing If-Match, 503 service shutting down. Problems such as an unavailable source may currently be returned as 422; the MVP has not yet classified these as retriable network errors; clients should read `detail` and not retry unconditionally.

### Synchronized Position in Output

The `checkpoint` in an output response denotes the **complete transaction boundary to which the returned rows have been applied**; it has the same structure as the standalone checkpoint endpoint. The service generates rows and checkpoint from the same manifest history under the same storage lock; it never pairs old data with a new position.

- `sequence` is the committed transaction ordinal within the instance, not a GTID; `transaction_id` is the identity of the corresponding source transaction.
- `position.file/position` is the resume position after that commit, not the position the listener has read but not yet committed.
- Before the instance has committed anything, rows are empty, sequence is 0, and position is the initial incremental starting point; this does not mean data before the initial point has been exported.
- Fully filtered transactions, unrelated-table transactions, and safely skippable unrelated DDL can also advance the checkpoint; rows may not change.
- Updates are asynchronous; after the SQL succeeds, poll the output and check the target values or position. You can compare against the source position obtained via `SHOW BINARY LOG STATUS` after the SQL commit; compare the position within the same file, and across files compare by source binlog file order, not by numeric offset alone.
- The checkpoint does not guarantee that the source has produced no new data by the time you query MySQL again, nor that any single GET has caught up with the source.

## 8. Pause, Resume, Errors, and DDL

```bash
curl -sS -X POST "http://127.0.0.1:8080/v1/instances/$INSTANCE_ID/pause" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: pause-1' -d '{}'
# Poll GET instance until actual state=PAUSED before doing anything else
curl -sS -X POST "http://127.0.0.1:8080/v1/instances/$INSTANCE_ID/resume" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: resume-1' -d '{}'
```

States are STARTING, RUNNING, PAUSING, and PAUSED. A 202 only means the command has been recorded; pausing may cancel uncommitted transactions, and after resume the pipeline replays from the last committed point. Resuming while PAUSING returns 409. Function, output-validation, disk-write, or listener errors put the instance into PAUSED with `last_error` retained; bad events are not silently skipped and there is no automatic infinite retry. resume clears the previous error and retries fixed code; if the root cause persists, the instance pauses again.

After a normal service shutdown, instances with desired_state=RUNNING automatically recover on the next start with the same data directory; instances paused by the user remain paused. Source network errors pause the instance instead of automatically reconnecting. Purged binlogs, a changed source UUID, or a schema mismatch all refuse to continue and do not automatically switch to current.

**Controlled DDL**: on start/resume the full pinned schema is compared; at runtime the SQL AST is used to check which tables/databases the DDL references. Confirmed create/alter/drop/rename of tables and index operations that do not touch subscribed tables may continue, and a zero-output checkpoint is recorded along the normal commit path. DDL touching subscribed tables/schemas, statements that cannot be parsed, or statements that cannot be safely classified pause the instance and are not skipped. Pause the corresponding instance before running DDL on subscribed tables; the MVP has no schema version migration or consumption-point continuation, so an ordinary resume after a structural change will not automatically recover. During initialization, keep a DDL-free window.

## 9. File Layout and Checkpoint Semantics

```text
.local/etl/
  .server.lock
  catalog.json                     # functions, drafts, versions, instances, idempotent responses
  outputs/
    .localstore.lock
    inst_.../
      00000000000000000001-<digest>-orders.jsonl
      manifest.json                # single commit truth and current checkpoint
```

Processing order for one complete source transaction: read COMMIT → all Lua batches → validate all mutations → write each output file (including empty outputs) and fsync → atomically replace the manifest and fsync the parent directory. The checkpoint and the file references live in the same manifest, avoiding a progress hole from committing them separately.

Replaying the same transaction, sequence, version, and byte-identical content returns the original commit result; changed content reports CONTENT_CONFLICT; a non-contiguous sequence reports SEQUENCE_GAP. Reads follow only manifest references and verify checksums, and never scan the directory to infer commits. An interruption between the file write and the manifest may leave unreferenced files behind; they never appear in rows, and no automatic GC is provided. Users must not modify files, nor infer that a transaction has been committed merely because some JSONL file exists.

Local guarantees are limited to single-process writes, an exclusive directory, and a local filesystem that supports the corresponding fsync/atomic rename. The replication stream may re-read events, but local commits are idempotent; do not claim cross-system exactly-once on this basis. Losing the data directory loses both results and recovery state.

Row queries apply upsert/delete from the full committed history with no index; the full manifest is rewritten on every commit, and the per-transaction/per-file/cumulative manifest limit is 64 MiB; memory and read costs grow with history, so this suits verification rather than long-term production. The listener caps each transaction at 10,000 subscribed events or roughly 16 MiB of standard events; exceeding this pauses the instance. There is no automatic disk capacity scaling or retention management.

## 10. Verification and Shutdown

```bash
make test                     # Go race unit tests, no database access by default
make e2e                      # real Compose MySQL + HTTP + Lua subprocess + temp disk
```

The E2E suite creates its only test table under `shop` and drops it at the end; before running, check that local port 13306 is this project's Compose MySQL. Coverage includes: incremental-only with no full load, exact large integers and decimals, complex transforms, updates and deletes, filtering, pause catch-up, restart recovery, idempotent requests, Lua/schema errors not advancing progress, subscribed-table DDL pausing, and unrelated-table DDL not blocking. See `e2e/README.md` for details.

Stop the service with Ctrl-C; stop MySQL with `make mysql-down`, which keeps the Docker volume. Do not delete `.local/etl` or the MySQL volume directly and then continue using old tasks. For a full reset, stop the service and back up the data first, then explicitly delete the local directory and the Compose volume yourself; the project's startup scripts never perform a destructive reset automatically.

## 11. Future Iceberg Integration Boundary

The target may still adopt Iceberg v2 + Parquet + S3/Catalog in the future, but this release does not simulate that capability. When the sink is replaced later, the success criterion must be a successful Iceberg snapshot commit, not the mere appearance of files; commit identity verification, catalog concurrency, type mapping, upsert/delete, and maintenance policies must also be implemented. The S3 backlog buffer is a separate persistence component with archival watermarks and delivery checkpoints; currently there is only an interface draft.

Unimplemented items in historical design and research documents — PostgreSQL, WASM/Lambda, archival, upgrades, automatic retries, manifest API, etc. — must not be interpreted as features supported by this API.
