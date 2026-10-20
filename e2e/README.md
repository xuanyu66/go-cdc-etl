# MySQL → Lua → Local Disk End-to-End Tests

Run from the repository root:

```sh
docker compose up -d --wait mysql
ETL_E2E=1 go test -tags integration ./e2e -count=1 -v
```

The tests call the real control plane HTTP handler via Go `httptest`, build `cmd/etl`, start real Lua worker subprocesses, consume binlog from a dedicated MySQL 8.4 container, and finally verify the committed files and checkpoints in a temporary directory. They do not mock MySQL, the transformer, or the local sink.

The tests create uniquely named test tables in the Compose `shop` database and drop them afterward; they do not modify the demo `shop.orders`. DDL on a subscribed table pauses the corresponding reader; unrelated-table DDL that can be safely classified no longer blocks other fixed-table pipelines. The tests themselves still run serially to avoid concurrent modification of the same test table. The HTTP server and the output directory are created and cleaned up by the tests; the MySQL container is left running.

Coverage:

- Export starts from the current position, incremental-only, with no records from before the instance was created.
- Lua conditionals, string operations, and exact decimal arithmetic; business keys beyond the JavaScript safe integer range are preserved losslessly.
- Multi-row writes, updates, and deletes within a transaction, state filtering, and retraction of stale outputs.
- Transaction checkpoints can still be committed when everything is filtered out.
- Progress does not change while paused; after resume, the backlog is processed.
- Restarting with the same data directory catches up on data written during downtime; committed files are neither duplicated nor rewritten.
- Replaying an identical idempotent create request returns the same resource response.
- Lua execution errors, output schema errors, and DDL on a subscribed table cause a pause, and the checkpoint does not advance past the failed transaction.
- Unrelated-table CREATE/ALTER/RENAME/DROP does not pause the subscription and can persistently advance the checkpoint.
- Control plane validation, function reads, draft queries and modification, optimistic concurrency checks, published versions, and instance management.
- Insert, update, and delete on two required outputs, plus HTTP error semantics for idempotency conflicts, unknown resources, and invalid definitions.

The tests are skipped when `ETL_E2E=1` is not set; without the `integration` build tag, the regular `go test ./...` does not compile them. The tests always use this project's Compose instance at `127.0.0.1:13306` and do not accept arbitrary remote databases, to avoid accidental interference with other environments.
