# S3 Sink and Checkpoint Semantics Research

Status: design research and interface notes; this does not mean a production adapter has been implemented. Verification date: 2026-10-19. This document supplements the [design](../design.md); the code contract is authoritative in the actual [checkpoint](../../pkg/checkpoint/checkpoint.go) and [sink](../../pkg/sink/sink.go) files.

## 1. First-Version Conclusions and Boundaries

The first version adopts **at-least-once execution + independent-attempt S3 objects + a metadata commit ledger + full-transaction checkpoints**. The persistent metadata Store here is a production requirement; the current in-memory reference implementation only validates the protocol, loses all state on restart, and cannot be used for real CDC.

- The Listener read position only indicates receive progress; the checkpoint indicates that all required branches of every prior complete transaction have been reliably completed.
- A `Receipt` indicates that a given Sink/batch output has been persisted; it does not mean the whole DAG has been committed, i.e. **persisted != globally committed**.
- Through `Store.Commit`, the Coordinator atomically checks all required writes, the current lease, the continuous transaction sequence, and the predecessor revision, then writes the commit record and advances the checkpoint together.
- There is no cross-system atomic transaction between S3 and the metadata database available to this design. If a crash happens after upload but before metadata commit, the file may already be visible, and recovery may also add duplicate files.
- The first version does not provide committed-manifest queries or transactional atomic visibility; consumers that scan S3 directly must tolerate uncommitted partial output and identify duplicates by stable event identity.

S3 provides strong consistency for reads of successfully written objects and atomic single-key updates; it has no cross-key atomic updates. This supports verifying upload results but cannot replace the DAG commit protocol. [S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)

## 2. Commit Units, Stable Identity, and Execution Order

**The commit unit is the complete source transaction, the execution unit is a batch within a transaction, and the physical output unit is a bounded object.** In the first version, each object belongs to exactly one transaction; multiple transactions are never mixed in one object. A large transaction may be split into multiple batches, but each pipeline executes batches in source-transaction and batch order and processes only one batch at a time. Different sources or independent instances may run in parallel; parallel batches, branches, and uploads within a transaction are follow-up extensions.

The transaction plan must pin the source generation, transaction identity, transaction sequence, recovery position, batch division, pipeline/transform version, and required writes. Retries, scaling, and redeployment must not change these logical identities. Each `Write` is `(SinkID, BatchID)`; branches that are fully filtered or receive no data must still have an explicit `Empty` receipt — the checkpoint must not be blocked forever just because no S3 object exists.

GTIDs identify source transactions; they are not globally increasing integers directly comparable across different source UUIDs. Commit order uses source sequence numbers assigned at receive-and-plan time; the recovery position stores the consumed set or the file/position after a complete transaction — do not substitute a single-event position. [MySQL GTID definition](https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html)

Event and output identity follows the design; outputs carry the pipeline, event ID, transform version, and output index. Multiple Sink outputs of a DAG should also retain Sink identity; configuration switches are isolated by immutable pipeline version and instance generation.

First-version object keys include `attempt_id`, consistent with the design. A new upload attempt or a recovery replay may produce a new key; therefore a receipt can record known objects but does not guarantee only one copy of the output exists in S3. Unknown-success objects after a timeout or crash may become unreferenced files. File naming or event IDs must not be treated as proof of exactly-once.

## 3. First-Version Commit Protocol and Fencing

```text
read complete transaction
  → Prepare immutable transaction plan and required sink/batch writes
  → transform and validate one batch in order
  → Sink.Persist uploads all objects of this batch
  → RecordReceipt records the persistence acknowledgment
  → continue with subsequent batches until all required writes complete
  → Store.Commit atomically writes commit record + checkpoint
```

All declared Sinks are required in the first version. When one Sink has succeeded and another fails, the checkpoint stays unchanged; known-success receipts can be retained to avoid duplicate writes, but retries of unknown results may still produce duplicate objects. Deterministic transform errors pause the pipeline; transient storage errors use bounded backoff.

In the production PostgreSQL Store, `Commit` locks the lease/checkpoint rows of the same instance in one transaction, checks the owner, increments the fence, the validity period under database time, the expected revision, the continuous sequence, and all receipts of the required writes. It then inserts a unique commit record, updates the checkpoint/revision, and commits. Takeover, renewal, and commit all follow the same row-lock protocol; S3 network calls happen outside the database transaction. For database row-lock capabilities, see the [PostgreSQL locking documentation](https://www.postgresql.org/docs/current/explicit-locking.html).

The database commit is the linearization point of the checkpoint. An old worker may still complete its S3 requests, but it cannot commit the metadata database record. Fencing protects the metadata; it **cannot revoke an already-issued S3 upload**. Losing the lease cancels scheduling immediately; late objects may become unreferenced files, which at-least-once semantics allows. If the same commit succeeded but the client response was lost, retrying with the same expected revision/sequence should return the already-committed result without advancing again.

## 4. Interfaces Consistent with the Code Framework

The following lists the responsibilities of the current interfaces; the actual signatures are authoritative in `pkg/checkpoint` and `pkg/sink`; no second compatible interface set is maintained.

| Type | Meaning |
| --- | --- |
| `Scope` | tenant, instance, and generation derived from authenticated ownership |
| `Lease` | owner, increasing fence, and validity period |
| `Cursor` | revision, continuous sequence, transaction ID, and ResumeAfter |
| `Write` | the identity of one required Sink/batch write |
| `Prepared` | immutable transaction plan: sequence, transaction ID, PlanDigest, recovery position, all required writes |
| `Receipt` | the object list of a Write, or explicit Empty; indicates persistence only |
| `CommitRecord` | Cursor, PlanDigest, and receipts after atomic commit |
| `Store` | lease management, Prepare, RecordReceipt, and Commit |

```go
// checkpoint.Store
Acquire(context.Context, Scope, string, time.Duration) (Lease, error)
Renew(context.Context, Lease, time.Duration) (Lease, error)
Load(context.Context, Scope) (Cursor, error)
Prepare(context.Context, Lease, uint64, Prepared) error
RecordReceipt(context.Context, Lease, uint64, Receipt) error
Commit(context.Context, Lease, uint64, uint64) (CommitRecord, error)

// sink.Writer
Persist(context.Context, Request) (checkpoint.Receipt, error)
```

`Prepare` and `RecordReceipt` are idempotent for identical content and reject the same logical identity with different content. `Commit` is validated atomically inside the Store; caller-side pre-checks cannot replace the storage constraints. The current error classification is `ErrFenced`, `ErrConflict`, `ErrIncomplete`, and `ErrInvalid`. The Sink does not commit checkpoints and does not claim whole-DAG success.

In the first version, `Prepared` pins the logical writes but does not persist the expected key, size, and digest of each physical object. The `Receipt` records this information after a successful upload. If strict object reuse is implemented later, a pre-upload object plan and recovery queries must be added; do not assume the current interface already provides that capability.

## 5. State Machine, Retries, and Recovery

```text
PREPARED → PERSISTING → PERSISTED → COMMITTED
                ↘ retry / PAUSED
```

`PERSISTED` means all required receipts have been recorded but the transaction is not yet committed; `COMMITTED` and the checkpoint update are in the same atomic operation. `PAUSED` is a runtime state that does not destroy the plan. A production Store must persist plans and receipts and resume incomplete transactions after takeover; the in-memory reference Store cannot provide crash recovery across process restarts.

| Failure point | Recovery action | Checkpoint |
| --- | --- | --- |
| Crash before the plan is persisted | Re-read from the old position and build the plan | Unchanged |
| Some batches succeeded | Reuse known receipts, continue the missing writes | Unchanged |
| Upload succeeded, receipt not recorded | An unknown success may produce duplicate files; retry per the original logical write | Unchanged |
| Sink A succeeded, Sink B failed | Keep A's known receipt, complete B | Unchanged |
| All receipts succeeded, crash before commit | Current owner verifies completeness, then commits atomically | Advances after commit |
| Commit succeeded, response lost | Retry the same commit, return the original result | Does not advance again |
| Late upload from an old worker | Cannot update the checkpoint; the file may become an orphan | New owner decides |
| Binlog has expired | Pause; requires a recovery plan or a new source generation | Do not fake progress |

Transient network errors and S3 5xx/throttling use the AWS SDK standard retry, with the attempt count and deadline configured explicitly; the outer layer uses jittered backoff with a total retry budget. Unlimited retries at both the SDK and Coordinator layers are not allowed. Permission and bucket-configuration errors should be classified as pause. A client timeout does not prove the remote request did not complete. [AWS SDK for Go v2 retries and timeouts](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-retries-timeouts.html)

SHA-256 records the actual compressed bytes; `ETag` is not treated as a general-purpose MD5. The upload verification algorithm must match the semantics of full-object/multipart checksums. The concrete first-version S3 upload adapter is still to be implemented. [S3 upload integrity checking](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)

The object retention period should cover consumption and recovery needs; objects still referenced by the ledger must not be deleted. Orphan cleanup must check active plans, commit references, and protection windows; the first version may provide manual audit first and not automatically delete unknown files.

## 6. Follow-up Candidates: Stable Object Keys, Conditional Writes, and Manifest

This section describes improvement directions; it is **not the first-version protocol or an implemented API**.

### Stable Object Keys and Conditional Writes

To reduce replay duplicates, a persisted pre-upload object plan can be added, pinning the key, byte count, SHA-256, record order, encoder version, and gzip header. Stable keys do not include a fence or attempt ID; the same object is reused across takeovers. A replay that produces different bytes must report a content conflict and must not silently overwrite. This requires changing the current Prepared/object plan and the recovery protocol, and verifying deterministic encoding.

Under this scheme, use `If-None-Match: *` on `PutObject`. A `412` must not be treated directly as success; verify the identity and real content digest of the existing object, reading the checksum or downloading to compute it when needed. A `409` on PUT is retryable; a `409` at multipart completion requires restarting the multipart. Conditional writes prevent overwrites, do not validate platform fencing tokens, and provide no cross-object transactions. [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

### Committed Manifest

A read-only API querying metadata commit records can be provided later, letting consumers read only the committed object set; the current OpenAPI does not implement these endpoints. If an S3 manifest is exported, a crash after uploading the manifest still does not prove the metadata was committed; a single source of commit truth and a database-outbox publication protocol must be defined. S3 LIST, file existence, or object notifications must not be used as commit evidence.

Even with the above optimizations, arbitrary downstream exactly-once cannot be claimed automatically. The read semantics through a manifest differ from directly enumerating S3 files and must be documented separately.

## 7. Verification Checklist

- Inject crashes before and after every state transition; verify no transaction is lost, no incomplete branch is skipped, and duplicate commits are idempotent.
- Two workers competing, lease expiry, and delayed uploads: verify an old fence cannot update the checkpoint.
- Large transactions in multiple batches, multiple Sinks, fully filtered branches, and branches with no routed data must all have complete required receipts.
- Lost response after a successful PUT: verify the retry permits duplicate files while the logical event identity stays stable.
- Switch versions before commit: verify pending transactions stay pinned to the original version and the new version takes effect at a complete-transaction boundary.
- In-memory reference tests can only prove the single-process atomic protocol; production PostgreSQL failure recovery and real S3 error behavior must be verified by separate integration tests.
- The conditional-write candidate is tested separately: 412 content match/conflict, 409, multipart rebuild, and deterministic encoding.

The commit mechanism above is this project's engineering design, not a cross-system transaction that AWS or PostgreSQL provides automatically.
