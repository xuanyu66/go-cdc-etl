# MySQL Listener Scaling and Internal API

Status: research and interface draft; verification date: 2026-10-19. This document is not a production implementation; the first-version scope follows the [project design](../design.md). The scaling strategies and interfaces below are recommendations for this project; the cited references only prove third-party capabilities.

## 1. Conclusion: Elastic Scaling Is Possible, but Reads and Execution Must Be Distinguished

A single MySQL instance's binlog does not natively provide independent consumption partitions like Kafka partitions. The first version uses one active reader per `pipeline instance + source`, parsing transactions in order; adding multiple readers for the same source with per-table filtering usually re-receives/re-scans the log, so read throughput cannot be claimed to scale linearly with replica count.

MySQL describes its replication chain as dump thread, receiver, and subsequent applier. Multi-threaded replication mainly parallelizes the apply phase; it does not mean multiple receivers automatically share the load of one log. [MySQL replication threads](https://dev.mysql.com/doc/refman/8.4/en/replication-threads.html). Debezium's MySQL connector also pins to a single task; this is a boundary reference from a mature implementation, not a theoretical prohibition on all custom read algorithms. [Debezium MySQL connector](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-tasks-max).

| Scaling layer | Scaling unit | Problems it can solve | Boundary |
| --- | --- | --- | --- |
| Reader worker pool | reader assignment for different sources/pipelines | more tenants, MySQL instances, or physical shards | one assignment can have only one valid owner |
| Single reader | CPU, network, decoding, and cache optimization | single-source read/parse bottleneck | memory cannot grow unboundedly; benchmark the single-stream ceiling separately |
| Transform worker pool | already-formed independent stateless calls | user-transform CPU cost and bursts | first version keeps one in-flight transaction per pipeline; concurrency comes from different pipelines |
| Sink worker pool | independent object upload tasks | compression/upload wait, cross-pipeline concurrency | a transaction can be committed only after all branches persist |
| Source physical sharding | independent MySQL logs | increase overall source throughput | requires business sharding or source-topology change; not achievable by the ETL tool splitting tables automatically |

Reading the full log from multiple replicas is usually for availability or isolating source load; it cannot be claimed to split the same binlog. Switching replicas must also verify GTID coverage, source logical identity, schema history, and the availability of the required binlogs.

The first version does not share readers across tenants: multiple pipelines on the same database may each read, with explicit connection/source pressure quotas. Sharing readers in the future requires reliable fan-out and independent progress per subscriber first; otherwise slow subscribers and log expiry will interfere with each other.

## 2. First Version and Follow-up Scaling Path

### First version: preserve transaction order, support multi-source elasticity first

```text
Scheduler → Reader assignment + lease
                          ↓
                    one ordered reader
                          ↓
                    committed transaction
                          ↓
             bounded transform / sink execution
                          ↓
               durable transaction completion
                          ↓
                    fenced checkpoint
```

Each pipeline holds only one uncommitted transaction; multiple pipelines are assigned to the worker pool and run in parallel. A read transaction may be split into multiple transform batches due to size, but the commit unit remains the complete transaction. This strategy is simple and consistent with the existing design; the cost is that a single pipeline cannot be accelerated indefinitely just by adding executor replicas. When a single large transaction exceeds the budget, it is explicitly paused; resource limits are not breached for the sake of scaling.

### Follow-up: single reader + bounded parallel execution

If the prototype proves the transform or S3 is the single-pipeline bottleneck, multiple in-flight transactions can be added:

1. The reader establishes stable transaction identity, source position, and batch plans in order; batch plans are persisted first.
2. Stateless transactions/batches may execute in parallel; completion status tracks all branches of the associated DAG.
3. The checkpoint coordinator advances only the **continuously completed transaction prefix in source order**: when `T1` is incomplete, `T2` and `T3` cannot be committed up to `T3` even if they are complete.
4. The incomplete window is bounded by transaction count, byte count, and the age of the oldest transaction; when a slow transaction hits the window limit, dispatch stops, preventing completion status and output from growing without bound.

The order here is the observed source transaction order. A GTID set is not a monotonically comparable integer; prefix advancement cannot be implemented by string sorting or taking the "maximum GTID"; the source traversal order and the corresponding recovery positions must be maintained.

This extension preserves complete-transaction checkpoints but does not provide the physical appearance order of S3 objects. When downstream transaction visibility is needed, use a commit manifest or a lake-table protocol; upload completion order must not be treated as commit order.

### Sharding and ordering

The first version has only `source_order` and does not provide user-defined key partitioning. If partitioning by primary key is supported later, the guarantee scope must be stated explicitly: same key ordered, different keys with no global order. Primary-key updates should map dependencies of the old key and the new key; a transaction spanning multiple keys/partitions still requires merged acknowledgment. Tables without primary keys need a stable-key strategy; otherwise ordering by business record cannot be promised. Changing `hash(key) % worker_count` directly after scaling changes routing; stable virtual partitions should be used, and ownership migrated after partitions are drained.

## 3. Lease, Fencing, and Migration

The assignment key is `(tenant_id, pipeline_id, instance_id, source_id)`; source generation and output generation are separate persistent identities and must not be conflated with short-term worker leases.

- The metadata database obtains the assignment lease by conditional update and returns a strictly increasing `fencing_token`. Renewal and commit both match the owner and token and check the validity period under database time.
- For the prototype, a lease TTL of 30 seconds with renewal every 10 seconds is recommended; when renewal capability is lost, stop pulling and cancel execution as early as possible. These are starting points before fault injection, not production-availability promises.
- All checkpoint and transaction-completion writes validate the fencing token inside a metadata database transaction; an old worker cannot advance any commit state even if its network recovers.
- In-process cancellation, a unique `server_id`, and Kubernetes leader election cannot replace fencing at metadata-write time.
- S3 does not automatically understand database tokens. An old worker may leave complete or incomplete extra objects; objects need attempt identity, and a published manifest or committed metadata may only reference objects confirmed by the valid owner.
- Normal scale-in: mark draining, stop acquiring new transactions, finish and commit the current transaction, close the replication connection, and release the lease. If the drain deadline is exceeded, terminate and let the new owner replay from the committed checkpoint; duplicates are allowed, and the read position must not be skipped to.
- Worker crash: a new worker waits for the lease to expire, acquires a new token, reads the persisted checkpoint, verifies source continuity, and resumes. Incomplete local staging is discarded.

## 4. Backpressure, Metrics, and Scaling Control

The MySQL binlog itself remains the first-version re-read source; in-memory channels, local spools, and replication-library buffers are not treated as reliable queues. Set byte limits separately for the application queue, the library-internal queue, and local staging; after the application stops pulling, the library may still read briefly, and backpressure on the upstream should ultimately be applied through bounded buffering and TCP. Debezium's bounded blocking queue is a mature backpressure mechanism worth referencing. [Debezium queue configuration](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-max-queue-size).

| Metric | Purpose |
| --- | --- |
| input bytes/s, events/s, transactions/s | distinguish large-row, large-transaction, and high-event-count loads |
| reader CPU, decode latency, network throughput, reconnect count | locate read-side bottlenecks |
| waiting assignments, active assignments | decide whether the reader pool needs more capacity |
| queue bytes / limit, spool bytes / limit | enforce backpressure and memory/disk protection |
| oldest uncommitted transaction age, checkpoint age | identify actual delivery lag and blocking |
| read position vs committed position | distinguish read lag from downstream lag; positions must not be subtracted directly across files |
| transform/sink queue age, latency, errors, throttles | decide whether to scale the execution side or fix the target bottleneck |
| lease loss, fence reject, drain duration | verify scale-in and failover |
| oldest available binlog / required checkpoint | judge the actual replay gap and retention risk |

Source event time is only used for approximate lag; idle sources must be distinguished from real backlog, and source clock skew must be considered. An idle database must not be scaled to the maximum based solely on "how long ago the last event was".

Recommended control policy:

1. The reader pool scales based on pending assignments, weighted assignment load, and CPU; if only one hot source's read side is saturated, mark it `SOURCE_READER_SATURATED` instead of loop-creating useless replicas.
2. The executor pool estimates concurrency from schedulable backlog and service time, then is constrained by tenant concurrency, maximum replicas, and downstream quotas; when S3 is throttled, back off first instead of continuously adding upload concurrency.
3. The prototype evaluates once per 30-second smoothing window and scales out only after two consecutive high-load windows; scale-in observes 5 minutes of low load and drains one by one. Concrete thresholds are determined by benchmarks, and the user API must not expose unverified fixed-capacity guarantees.
4. Kubernetes deployments can expose these queue/assignment metrics to the `autoscaling/v2` HPA; HPA itself supports custom metrics and stabilization windows, but it does not handle source assignment, transaction draining, or fencing — those remain this service's responsibility. [Kubernetes HPA](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/).
5. Binlog retention must cover the longest failure and catch-up window; the configured expiry time is not an actual retention guarantee, and manual cleanup must also be detected. If the recovery point has been purged, enter `NEEDS_RESEED` instead of switching to latest. [MySQL binlog retention](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_expire_logs_seconds).

## 5. Go Internal API Draft

The following are responsibility descriptions; the unified code framework follows the project's Go types. `CDCEvent` uses the lossless types of the main design; source connection parameters are exposed in the external API as credential references, not plaintext.

```go
type SourcePosition struct {
    Mode         string // "gtid" or "file_position"; validate as a tagged union
    GTIDSet      string // consumed transaction set, not the current transaction GTID
    File         string
    NextPosition uint64 // resume after a committed transaction
}

type Transaction struct {
    ID       string
    Events   []CDCEvent // bounded; larger transaction may use an owned spool handle
    Resume   SourcePosition
    Bytes    int64
}

type OpenRequest struct {
    SourceID    string
    Generation  string
    Position    SourcePosition
    Credential  string // resolved secret reference, never passed to a transform
    ServerID    uint32 // allocated uniquely for concurrent MySQL connections
    MaxTxnBytes int64
}

type Listener interface {
    Open(ctx context.Context, req OpenRequest) (Stream, error)
}

type Stream interface {
    Next(ctx context.Context) (Transaction, error)
    Close() error
}

type Lease struct {
    Assignment string
    Owner      string
    Token      uint64
    ExpiresAt  time.Time
}

type LeaseStore interface {
    Acquire(ctx context.Context, assignment, owner string, ttl time.Duration) (Lease, error)
    Renew(ctx context.Context, lease Lease, ttl time.Duration) (Lease, error)
    Release(ctx context.Context, lease Lease) error
}
```

Contract:

- `Next` returns only complete committed transactions; unsubscribed rows are filtered before normalization but transaction boundaries are still recognized. ROLLBACK produces no committed business events; DDL takes the safe-pause error path and must not be disguised as an ordinary transaction. Heartbeats/rotations are handled internally; they are observable but produce no user row records.
- Calling `Next` does not write checkpoints; replication-protocol ACKs must not be treated as final S3 delivery acknowledgments. The checkpoint coordinator is the sole progress committer.
- Each stream may be called by only a single goroutine via `Next`; `Close` must be able to cancel in combination with blocked reads and must close idempotently. The cancellation semantics of `Next` and the reconnect policy after connection interruption are tested uniformly in the adapter.
- `Open` failures must distinguish `SOURCE_UNAVAILABLE`, `INVALID_POSITION`, `BINLOG_GAP`, `SCHEMA_REQUIRED`, and `INVALID_CONFIGURATION`; at runtime there are additionally `SCHEMA_CHANGED`, `TRANSACTION_TOO_LARGE`, and `LEASE_LOST`. Transient network outages may be retried; gaps and schema changes require explicit handling.
- Event identity must not depend on the worker, token, or the current retry; the same source event produces the same ID after a restart. `Lease.Token` only controls write permission.

The candidate Go library's replication adapter opens with `StartSync(mysql.Position)` / `StartSyncGTID(mysql.GTIDSet)`, pulls events via `BinlogStreamer.GetEvent(ctx)`, and closes with `BinlogSyncer.Close()`. The library provides raw replication capability; transaction assembly, schema history, fencing, and checkpoint coordination are implemented by this project. The official source has been reviewed via `gh api`; a pinned version is still required for formal adoption. [go-mysql replication API](https://pkg.go.dev/github.com/go-mysql-org/go-mysql/replication).

## 6. Acceptance and Next Steps

- After multi-source scaling, assignments spread out; one assignment never has two valid committers.
- A hot single-source read saturation is diagnosed correctly; adding idle reader replicas is not misreported as throughput improvement.
- Fault injection covers renewal interruption, old-owner recovery, new-owner takeover, and scale-in timeout; commits with old tokens are always rejected.
- When downstream stalls, all queues/staging stay bounded; oversized transactions pause; recovery has no silent omissions as long as the database retains enough binlogs.
- For the transaction sequence `T1/T2/T3`, even if a later extension allows out-of-order completion, the checkpoint does not pass `T1` while it is incomplete.
- Transactions with no output, fully filtered, or on unsubscribed tables can advance the safe position; DDL, rotation, reconnects, and binlog gaps have explicit states.
- GTID primary-switch and file-position recovery are tested separately; do not claim file-position can be migrated directly to another server.

It is recommended to finish the recovery and fencing prototype with a single active reader first, then test the single-source bottleneck. Introduce bounded parallelism and a reordered-completion ledger only when data proves the transform or upload blocks a single pipeline; when a longer failure buffer is needed, use the S3 durable buffer in the next section instead of introducing a full Kafka cluster directly.

## 7. Whether to Spill Binlogs to Cloud Storage Under Downstream Backlog

Yes, and it fits scenarios where downstream backlogs are often long but data must be moved out of the MySQL retention window quickly. An optional **S3 durable buffer** is recommended, archiving already decoded and normalized complete CDC transactions plus schema snapshots or immutable references, rather than uploading raw binlog files only. Raw binlogs may serve as an additional backup, but recovery requires matching schema, decoder, source identity, and transaction boundaries, so they are not suitable as the executor's only input protocol.

```text
direct:
  MySQL → Listener → bounded execution → output S3 → delivered checkpoint

buffered:
  MySQL → Listener → archive S3 + PostgreSQL catalog → archive watermark
                                           ↓
                             ordered catalog reader
                                           ↓
                          execution → output S3 → delivered checkpoint
```

### 7.1 The Two Watermarks Must Not Be Mixed

| Watermark | What it means | Who advances it | Restart use |
| --- | --- | --- | --- |
| archive ingestion watermark | complete transactions up to this position can be recovered continuously from the archive | Archive publisher | the Listener can continue reading MySQL from here |
| delivered checkpoint | transforms and all required outputs up to this position have been persisted and committed | Checkpoint coordinator | the executor continues reading the catalog from after this position |

A successful archive only means "the source data has been taken over", never "the user output is complete". In buffered mode, MySQL failure recovery depends on the former; end-to-end delivery status depends on the latter. Alerts present read lag, archive lag, and delivery lag separately. Direct mode has no reliable archive watermark; the Listener still recovers from the delivered checkpoint.

### 7.2 How Archive Objects and the Catalog Are Published

An archive segment may contain multiple complete transactions, bounded by post-/pre-compression bytes, transaction count, and a maximum seal time; a transaction that is not fully committed must not be announced as archived. An oversized transaction still respects the explicit size limit; unbounded caching is not excused by object storage's large capacity.

Each segment holds a protocol version, source and generation, stable segment ID, predecessor segment ID, start/end recovery positions, transaction index, event IDs, schema digest, compression method, length, and checksum digest. If schemas are stored separately, the immutable schema object must be written reliably first and then referenced; recording only the current database table structure is insufficient to replay historical transactions.

Publication order:

1. Upload the complete segment under an immutable object key and verify the upload result; conditional writes may be used to prevent same-name overwrites. Archive credentials are separate from user-output credentials, and user functions cannot read the archive.
2. In a PostgreSQL transaction, validate the ingestion lease/fencing, the expected old archive watermark, and the predecessor catalog entry; insert the segment catalog entry, then atomically advance the archive watermark. If any condition fails, the segment is not published.
3. S3 objects and PostgreSQL cannot commit atomically across systems; the so-called "atomic publish" means the catalog entry and the watermark are committed together in PostgreSQL and only reference S3 objects confirmed to exist. A crash after upload but before database commit leaves orphan objects but produces no published-catalog gap.
4. Retry of a failed publication checks against the stable segment identity and digest; an existing identity with different content is an error. An old owner may leave orphans but has no right to update the valid catalog.

S3 supports object-level strongly consistent reads and writes, and conditional writes can prevent accidental overwrites, but these do not provide a distributed transaction between S3 and PostgreSQL. [S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel), [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html). Catalog contention control may use row locks or conditional updates; the predecessor relationship and the watermark are always maintained in the same database transaction. [PostgreSQL transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html).

The executor takes segments in the order of the predecessor relationship/published sequence from the PostgreSQL catalog, validates the source generation, transaction index, and checksum digest, and then resumes from the delivered checkpoint. **Using S3 LIST to sort and concatenate the data stream is forbidden**: even if LIST is strongly consistent, it cannot distinguish orphan objects, old-owner attempts, and the published continuous prefix; object-name sorting cannot replace GTID semantics either.

### 7.3 Continuous Buffered Mode Beats Temporary Spillover

It is recommended to choose `direct` or `buffered` explicitly at pipeline startup. Buffered mode always archives before executing, keeping the path and recovery rules fixed; it does not temporarily spill the second half to S3 when a queue exceeds a threshold and then chase live data directly after recovery. Such automatic spillover requires extra guarantees that there are no holes, duplicates, or overruns between the switch boundary, in-flight data, and the archive stream; the benefit is usually not worth the complexity.

Mode switching is allowed only after an explicit pause, drain, and recording of the complete-transaction boundary; the first version may simply reject in-flight switching. This round only adds the durable buffer's responsibilities, interface boundaries, and documentation; the archive/replay backend is not implemented; `direct` remains the existing first-version reference path.

### 7.4 Cleanup, Quotas, and Cost

- Cleanup is based on the minimum continuous position across the delivered checkpoints of all valid subscribers, subject to a minimum replay retention period; data not yet delivered must not be deleted by the archive watermark.
- Initially each pipeline archives independently; when source archives are shared in the future, subscriber registration, deregistration, and the slowest progress must be incorporated into the GC protocol. A subscriber that is individually broken may only be paused, alerted, or have its history explicitly abandoned by the user; silently skipping is not allowed.
- GC first marks candidates in the metadata database and coordinates with new-subscription/replay pins, then deletes objects and records the result; schemas are reclaimed only after no active segment/replay references remain. Orphan objects must pass a recovery safety period and be checked against the catalog; newly uploaded objects must not be deleted immediately.
- Set per-tenant alerts for buffer bytes, segment count, maximum delivery lag, and cost budget. When a hard quota is reached, pause ingestion and rely on alarms about the MySQL remaining retention period; if the required logs have expired, enter `NEEDS_RESEED`.
- Lifecycle rules must not unconditionally delete undelivered buffers by a fixed number of days; business deletability should be controlled by catalog GC. The archive itself also requires backing up the catalog and protecting schema and data objects; backing up only S3 data while losing the catalog is not recoverable.
- Costs include extra S3 PUT/GET/storage, the PostgreSQL catalog, compression and encryption/decryption, network, and replay compute. Larger segments reduce request costs but increase seal latency, replay read amplification, and per-retry cost; small transactions should not default to one object per transaction. Unit prices are accounted by deployment region. [S3 pricing](https://aws.amazon.com/s3/pricing/).

The durable buffer solves time decoupling between source reading and downstream execution; it neither breaks through single-source reader throughput nor lets an executor running persistently below the input rate catch up automatically. When backlog keeps growing, the executor still must be scaled, transform efficiency improved, tenant input controlled, or target storage scaled up.

Supplementary acceptance covers: crash after upload but before catalog commit, unknown catalog-commit result, publication with an old ingestion token, missing objects/digest errors, lost schema, slowest subscriber blocking GC, archiving stopped at quota, and the delivered checkpoint advancing correctly after replay from the archive.
