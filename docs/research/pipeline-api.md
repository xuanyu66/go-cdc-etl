# Data Pipeline Orchestration and API Research

Verification date: 2026-10-19. This document proposes interface design and does not mean the service has been implemented; see [design.md](../design.md) for the project overview. This round delivers the OpenAPI, JSON Schema, and a Go code skeleton; the CLI implements Go-structure and graph validation, but a generic JSON Schema validator is not yet integrated, and HTTP handlers, resource authorization, and connection testing are not implemented.

## 1. Recommended Approach and Standard Boundaries

The approach is **OpenAPI 3.1.1 + JSON Schema 2020-12 + a small declarative dataflow definition**. HTTP interfaces use JSON; the code-framework CLI in this round also uses JSON, and JSON documents themselves are valid YAML 1.2. A general YAML input adapter is left for later; arbitrary YAML file support is not claimed. This way users only need to understand sources, transforms, sinks, and the connections between them, without first learning a general workflow engine.

No generic DAG standard was found that directly unifies MySQL CDC, continuous data streams, transaction boundaries, branch acknowledgment, and S3 checkpoints. The following protocols must be defined explicitly by this project; they cannot be obtained automatically by picking a YAML look.

| Candidate | What it actually standardizes | This project's trade-off |
| --- | --- | --- |
| OpenAPI | HTTP paths, request/response, auth, API documentation, and client generation | used to describe the control plane; not responsible for DAG execution or checkpoints. [Official spec](https://spec.openapis.org/oas/v3.1.1.html) |
| JSON Schema | JSON document structure, field types, and local constraints | used for IDE hints and structural validation; cycles, reachability, and resource access permissions are still validated by the service. [Official spec](https://json-schema.org/draft/2020-12) |
| CNCF Serverless Workflow / Open Workflow Specification | declarative workflows for service invocation, event-driven, and control flow | the official site currently presents the Open Workflow Specification, marked as DSL 1.0.3. Definition/instance separation is worth borrowing, but the full DSL is not adopted directly; CDC continuous running and acknowledgment semantics require extra customization. [Official site](https://open-workflow-specification.org/) |
| Argo Workflows | DAGs of task-completion dependencies on Kubernetes | `dependencies` means the downstream runs after the upstream task completes, not that data flows continuously along edges; no per-CDC-event Pod or Workflow creation. [Official DAG docs](https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/) |
| Redpanda Connect (Benthos lineage) | YAML-described input, processors, output, and their product execution behavior | closest to the streaming data-integration experience; borrow component configuration and the input/output model; it is concrete product configuration, not a cross-engine CDC standard. [Configuration](https://docs.redpanda.com/connect/configuration/about/), [processing pipelines](https://docs.redpanda.com/connect/configuration/processing_pipelines/) |

For configuration semantics, nodes/edges are chosen instead of offering both a processors shorthand and a full-graph DSL, avoiding two equivalent syntaxes for users. The future graph editor reads and writes the same definition directly. v1 introduces no expression language; filtering and field mapping use transform functions, avoiding learning a Lua/WASM SDK plus another expression DSL at the same time.

## 2. Definitions, Versions, and Running Instances

- **Pipeline Definition**: the editable configuration, with `pipeline_id` and a monotonically increasing `revision`.
- **Pipeline Version**: the immutable snapshot produced at publish time, with `version_id`, a normalized configuration digest, and a fixed transform-artifact digest; configuration defaults are expanded at publish time.
- **Pipeline Instance**: a long-running instance of a given version, with `instance_id`, output `generation`, state, checkpoint, and execution lease. Process restart or migration does not create a new output generation.

Editing the draft does not affect running instances. In v1 an instance's version is fixed; pausing and resuming does not modify the version, source start position, or output generation. This round does not provide upgrade/stop APIs. Later upgrades, failed-transaction repair, and historical recomputation require separate processes that must make explicit how uncommitted transactions, sources, and output generations are handled; they cannot be achieved by overwriting a published version.

This round's lifecycle is defined around `STARTING → RUNNING → PAUSING → PAUSED → RUNNING`. Recoverable errors enter `PAUSED` with a reason; a source binlog gap enters `NEEDS_RESEED` and must not be silently skipped via ordinary resume. Pausing completes only at a transaction-safe boundary; after a crash, recovery resumes from the last committed checkpoint.

## 3. Minimal Dataflow Configuration

The fields below are this project's proposed format; compatibility with Kubernetes, Argo, or Serverless Workflow is not claimed.

```yaml
api_version: v1
kind: Pipeline
metadata:
  name: orders-export
spec:
  nodes:
    - id: source
      type: source.mysql
      mysql:
        connection_ref: mysql-production
        tables: ["shop.orders"]
        start:
          mode: gtid
          gtid_set: "24bc7850-2c16-11ef-9f31-0242ac120002:1-41"
    - id: project
      type: transform.wasm
      wasm:
        artifact_ref: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
        timeout_ms: 5000
        memory_mib: 64
    - id: archive
      type: sink.s3
      s3:
        connection_ref: s3-archive
        bucket: company-archive
        prefix: orders
        format: jsonl
        gzip: true
  edges:
    - from: source
      to: project
    - from: project
      to: archive
```

`connection_ref` is a pre-registered, tenant-authorized resource identifier; `artifact_ref` is an immutable `sha256:<64 lowercase hex>` identifier; the definition must not contain passwords, access keys, or mutable remote code locations. A full service publish needs to verify the artifact digest exists, its ownership, and the connection type and target authorization scope; these resource validations are not yet implemented. Connection credentials may be rotated, but credential rotation must not change the source identity or the target bucket.

Field specification:

| Field | Constraints and semantics |
| --- | --- |
| `api_version`, `kind` | fixed to `v1` and `Pipeline`; no compatibility branches are added for unreleased formats |
| `metadata.name` | regex `^[a-z][a-z0-9_-]{0,62}$`; uniqueness within a tenant is guaranteed by the later control plane |
| `spec.nodes[].id` | same character rules, unique within the graph; a semantic identifier — nodes are not identified by array index |
| `type` | v1 implements `source.mysql`, `transform.wasm`, `sink.s3`; Lua/Lambda are candidates, and not-yet-implemented node types are rejected |
| `mysql.tables` | at least one exact `database.table` string, e.g. `["shop.orders"]`; the first version does not support wildcards or dynamically matching new tables |
| `s3.prefix` | a relative business prefix within the target connection's authorized scope; the platform additionally adds tenant/pipeline/generation identity and must not overwrite other tenants' paths |
| `spec.edges[]` | `from` and `to` reference nodes; an edge carries a record stream, not "start the downstream after the upstream task finishes" |

v1 graph constraints:

1. Exactly one source, at least one sink, and no self-loops, duplicate edges, or cycles.
2. The source has in-degree zero and sinks have out-degree zero; every non-source node has in-degree exactly one.
3. Every node is reachable from the source and can reach some sink; all branches are delivery-required branches.
4. Forking is allowed, replicating the same input to each branch; merges, joins, window aggregations, and stateful nodes are forbidden. Concurrent branches do not change the unified commit boundary of the source transaction.
5. A transform may produce zero to multiple outputs; when filtering yields zero, the completion acknowledgment is still passed on — downstream sinks must not wait forever.
6. The prototype limits each path to at most one transform node. The transform protocol in the current `design.md` uses the source event ID as the input correlation identifier; multi-level splitting would make that identifier non-unique, so multi-level transforms must first introduce platform-generated per-level record IDs and a lineage protocol before being enabled; the source event ID must not be reused directly as the per-level unique key.

The first version does not ask users to configure checkpoint algorithms, internal buffers, worker counts, or acknowledgment modes. Runtime resources are managed by platform quotas; later performance parameters must be separated from the business graph version to prevent scaling from accidentally changing processing logic.

## 4. HTTP Control Plane Specification Proposal

Uniform prefix `/v1`; requests and success responses are `application/json`. A Bearer token determines tenant identity; the body does not accept a `tenant_id` that overrides identity. Resources are invisible across tenants. The table below is the target contract and does not mean the endpoints are implemented.

| Method and path | Purpose | Success response |
| --- | --- | --- |
| `POST /v1/pipelines/validate` | validate and compile the complete Pipeline document; does not save and does not connect to the user's MySQL; resource authorization to be implemented separately | `200`, the compiled `Plan` |
| `POST /v1/pipelines` | create a draft; body is the complete Pipeline document | `201`, resource body, `Location`, `ETag` |
| `GET /v1/pipelines?limit=20&cursor=...` | cursor pagination, returning `items`, `next_cursor` | `200` |
| `GET /v1/pipelines/{pipeline_id}` | read the draft and revision | `200`, `ETag` |
| `PUT /v1/pipelines/{pipeline_id}` | fully replace the draft; requires `If-Match` | `200`, new revision and `ETag` |
| `POST /v1/pipelines/{pipeline_id}/versions` | body is `{"draft_revision":"3"}` (decimal string); validates and publishes an immutable snapshot | `201`, version resource |
| `GET /v1/pipelines/{pipeline_id}/versions/{version_id}` | read the fixed configuration and the resolved dependency digest | `200` |
| `POST /v1/pipelines/{pipeline_id}/instances` | create an instance with a fixed version and a validated start position | `201`, initial `STARTING` state and `Location` |
| `GET /v1/instances/{instance_id}` | return actual/desired state, error summary, current version | `200` |
| `POST /v1/instances/{instance_id}/pause` | drain to a transaction boundary; body is `{}` | `202`, returns the instance for polling |
| `POST /v1/instances/{instance_id}/resume` | resume from the current checkpoint; body is `{}` | `202`, returns the instance for polling |
| `GET /v1/instances/{instance_id}/checkpoint` | read-only consumption progress and source identity; does not expose the lease token | `200` |

Pause and resume are asynchronous state transitions; `202` only means the request has been durably accepted, and the user confirms the final state via the instance GET. Repeating a request for the same target state returns the current instance; a conflict with an in-flight transition returns `409`. A failed pause may retain the failure reason; HTTP acceptance must not be treated as drained.

Create-instance request example:

```json
{
  "version_id": "ver-001",
  "schema_baseline_ref": "schema-001"
}
```

The source definition's `mysql.start.mode` is `gtid` or `file_position`; the former requires `gtid_set`, the latter requires `file` and a decimal-string `position`, and the two field sets are mutually exclusive. A GTID set is the consumed set; processing continues from never-consumed transactions; file/position is the next-event boundary after the last completed transaction. The start position belongs to the fixed version; instance creation resolves the source identity and validates the schema baseline, and a historical start position lacking matching schema history is rejected. Ordinary resume does not accept a new start position; restarting an existing instance always uses its checkpoint and never re-applies the initial start position from the definition.

Write requests use the project-defined `Idempotency-Key`, storing a request digest and result keyed by tenant, method, normalized path, and key; retrying an identical request returns the identical result, while reusing the key with a different body returns `409`. It must cover at least create, publish, and lifecycle commands; a fixed retention period of 24 hours is recommended and should be written into the formal OpenAPI; clients must not treat expired keys as a permanent deduplication mechanism. `If-Match` is used for draft concurrent editing: missing the condition returns `428`, and a revision mismatch returns `412`. The two solve request retries and concurrent updates respectively and do not replace each other.

### Error Responses

Use [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457.html) with media type `application/problem+json`; `errors`, `code`, and `request_id` are project extension fields. `pointer` is a JSON Pointer into the request JSON; passwords, row data, and internal stacks are not returned.

```json
{
  "type": "urn:etl:problem:invalid-pipeline",
  "title": "Invalid pipeline definition",
  "status": 422,
  "detail": "Pipeline validation failed.",
  "code": "INVALID_PIPELINE",
  "request_id": "req-001",
  "errors": [
    {
      "pointer": "/spec/edges/1/to",
      "code": "UNKNOWN_NODE",
      "detail": "Referenced node does not exist."
    }
  ]
}
```

Status code conventions: malformed JSON is `400`; authentication failure `401`; insufficient permission `403`; invisible resource `404`; state/idempotency conflict `409`; concurrent version conflict `412`; request body too large `413`; well-formed but structurally or graph-invalid `422`; rate limited `429`; transient infrastructure failure `503`. Retry advice should be returned via `Retry-After`; clients must not unconditionally retry deterministic configuration errors.

## 5. Compiler and Execution Interface Boundaries

Rough code modules: `pipeline` holds definitions and the compiler; `control` manages drafts, publishing, and instances; `runtime` executes compiled plans; `checkpoint` coordinates acknowledgment of all required branches. The Go first version may be implemented within a single service; interface boundaries do not imply microservice boundaries.

```go
type Compiler interface {
    // Validate schema, graph, references and supported capabilities.
    Compile(ctx context.Context, definition Definition) (Plan, error)
}

type InstanceManager interface {
    Create(ctx context.Context, req CreateInstanceRequest) (Instance, error)
    SetDesiredState(ctx context.Context, id string, state DesiredState) (Instance, error)
}

type Runtime interface {
    // Run a long-lived plan under an acquired fenced execution lease.
    Run(ctx context.Context, plan Plan, lease ExecutionLease) error
}
```

These are interface sketches; the actual types are unified in the code framework. A `Plan` pins at least the version, topological order, node configurations, dependency digest, and the required sink set. The runtime handles data; the control plane does not schedule record by record; one binlog transaction corresponds to completion records of multiple batches and multiple sinks, and the orchestration graph is only its delivery path.

Target compile order: strict JSON parsing → JSON Schema validation → node/edge reference and topology validation → quota and resource authorization → dependency pinning → normalized plan generation. The current CLI covers only part of this with Go types, strict field parsing, and structural/graph checks; the JSON Schema files serve the contract and tooling consumption and do not mean the CLI has been validated by a generic schema engine. Arbitrary remote `$ref` fetching and duplicate keys are disabled; a future YAML adapter must also disable custom tags and limit document size and nesting to prevent editors and servers from interpreting configurations differently. Connection testing is executed separately; `validate` must not silently open a replication connection.

## 6. Verification Checklist

- The later YAML adapter must compile to the same plan as the corresponding JSON; the current JSON rejects unknown fields, duplicate keys, and invalid node configurations explicitly.
- Cycles, multiple sources, merges, dangling nodes, duplicate edges, unknown nodes, and multi-level transforms are rejected per v1 capability.
- Editing the draft after publish does not affect old versions; instance rescheduling does not change the version or source start position.
- Empty filtering still completes all required branches; one failing sink must not advance the transaction checkpoint.
- The same idempotency key does not create duplicate instances; reusing the key with a different request errors; concurrent draft updates return `412`.
- Pause's `202` is distinguished from the final `PAUSED`; pause/resume keeps the instance version fixed.
- Resource references, error logs, and instance queries must not leak across tenants; users must not be able to choose another tenant's identity via the request body.

The candidate standards and official pages in this document were verified as of 2026-10-19; version numbers only describe this observation and do not mean project dependencies will float to follow the latest versions.
