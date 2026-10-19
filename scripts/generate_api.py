"""Generate the reviewable API artifacts for the implemented local MVP."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def text(**kw): return {"type": "string", **kw}
def obj(props, required=None):
    return {"type": "object", "additionalProperties": False, "properties": props,
            "required": list(props) if required is None else required}
def array(items, **kw): return {"type": "array", "items": items, **kw}
def integer(**kw): return {"type": "integer", **kw}
def ref(name): return {"$ref": "#/components/schemas/" + name}
def write(name, value):
    (ROOT / "api" / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")

identifier = text(pattern="^[a-z][a-z0-9_-]{0,62}$")
nonempty = text(minLength=1)
column = obj({"name": identifier, "type": text(pattern=r"^(string|bool|int64|uint64|float64|decimal\([1-9][0-9]?,[0-9]{1,2}\))$"), "required": {"type": "boolean"}})
source = obj({"connection_ref": {"const": "mysql-local"}, "tables": array(text(pattern=r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$"), minItems=1, maxItems=16, uniqueItems=True), "start": obj({"mode": {"const": "current"}})})
lua = obj({"function_ref": nonempty, "timeout_ms": integer(minimum=1, maximum=5000)})
local = obj({"output": identifier, "key_fields": array(identifier, minItems=1, uniqueItems=True), "fields": array(column, minItems=1, maxItems=100)})
nodes = []
for kind, field, config in [("source.mysql", "mysql", source), ("transform.lua", "lua", lua), ("sink.local", "local", local)]:
    nodes.append(obj({"id": identifier, "type": {"const": kind}, field: config}))
pipeline = {"$schema": "https://json-schema.org/draft/2020-12/schema", "title": "Local CDC Pipeline v1", "description": "Implemented MVP: exactly one source -> one Lua -> 1..16 local sinks. Graph and decimal precision/key constraints also require semantic validation.", **obj({"api_version": {"const": "v1"}, "kind": {"const": "Pipeline"}, "metadata": obj({"name": identifier}), "spec": obj({"nodes": array({"oneOf": nodes}, minItems=3, maxItems=18), "edges": array(obj({"from": identifier, "to": identifier}), minItems=2, maxItems=17, uniqueItems=True)})})}
write("pipeline.schema.json", pipeline)
position = obj({"file": text(), "position": integer(minimum=0, maximum=4294967295)})
nullable_row = {"type": ["object", "null"], "additionalProperties": True}
mutation = {"oneOf": [obj({"output": identifier, "op": {"const": "upsert"}, "key": {"type": "object", "minProperties": 1}, "value": {"type": "object"}}), obj({"output": identifier, "op": {"const": "delete"}, "key": {"type": "object", "minProperties": 1}})]}
event = obj({"event_id": nonempty, "source_id": nonempty, "transaction_id": nonempty, "row_index": integer(minimum=0), "database": nonempty, "table": nonempty, "op": {"enum": ["insert", "update", "delete"]}, "before": nullable_row, "after": nullable_row, "columns": array(obj({"name": text(), "type": text(), "required": {"type": "boolean"}})), "source_position": position})
write("transform.schema.json", {"$schema": "https://json-schema.org/draft/2020-12/schema", "title": "Lua transform(event) input and mutation output", "description": "Lua returns a continuous array of mutations. Exact integer/decimal values are strings. Null maps to etl.null. This schema describes user contracts, not internal worker IPC.", "$defs": {"event": event, "mutation": mutation, "result": array(mutation)}, "oneOf": [{"$ref": "#/$defs/event"}, {"$ref": "#/$defs/result"}]})
plan = obj({"digest": text(), "source_node": text(), "transform_node": text(), "required_sinks": array(text()), "definition": ref("Pipeline")})
schemas = {
 "Pipeline": {"$ref": "./pipeline.schema.json"}, "Plan": plan,
 "FunctionRequest": obj({"name": nonempty, "source": text(minLength=1, maxLength=65536)}),
 "Function": obj({"function_id": text(), "name": text(), "source": text(), "digest": text()}),
 "Draft": obj({"pipeline_id": text(), "revision": text(pattern="^[0-9]+$"), "definition": ref("Pipeline")}),
 "Version": obj({"version_id": text(), "pipeline_id": text(), "draft_revision": text(), "plan": ref("Plan")}),
 "Instance": obj({"instance_id": text(), "pipeline_id": text(), "version_id": text(), "state": {"enum": ["STARTING", "RUNNING", "PAUSING", "PAUSED"]}, "desired_state": {"enum": ["RUNNING", "PAUSED"]}, "last_error": text(), "initial_position": position, "source_id": text(), "replication_server_id": integer(), "schemas": {"type": "object", "additionalProperties": array(obj({"name": text(), "type": text(), "required": {"type": "boolean"}}))}}, ["instance_id", "pipeline_id", "version_id", "state", "desired_state", "initial_position", "source_id", "replication_server_id", "schemas"]),
 "Checkpoint": obj({"sequence": integer(minimum=0), "transaction_id": text(), "version_id": text(), "position": position}),
 "Rows": obj({"rows": array({"type": "object", "additionalProperties": True}), "checkpoint": ref("Checkpoint")}),
 "DraftPage": obj({"items": array(ref("Draft"))}),
 "Probe": obj({"position": position, "server_uuid": text(), "tables": {"type": "object", "additionalProperties": array({"type": "object"})}}),
 "Object": {"type": "object"},
 "Problem": obj({"type": text(), "title": text(), "status": integer(), "code": text(), "detail": text(), "errors": array(obj({"pointer": text(), "message": text()}))}, ["type", "title", "status", "code", "detail"]),
}
errors = {str(n): {"description": desc, "content": {"application/problem+json": {"schema": ref("Problem")}}} for n, desc in [(400, "Malformed JSON or missing idempotency key"), (404, "Not found"), (409, "State, idempotency or name conflict"), (412, "Draft revision mismatch"), (413, "Request too large"), (415, "Unsupported media type"), (422, "Invalid definition, function or source"), (428, "If-Match required"), (503, "Server closing")]}
def op(name, output, code=200, body=None, mutation=False, ifmatch=False):
    result = {"operationId": name, "responses": {str(code): {"description": "Success", "content": {"application/json": {"schema": ref(output)}}}, **errors}}
    if body is not None: result["requestBody"] = {"required": True, "content": {"application/json": {"schema": body}}}
    if mutation: result["parameters"] = [{"name": "Idempotency-Key", "in": "header", "required": True, "schema": text(minLength=1, maxLength=128), "description": "Persisted for the local catalog lifetime, scoped to method/path. Replay requires byte-identical body and If-Match; conflict returns 409."}]
    if ifmatch: result.setdefault("parameters", []).append({"name": "If-Match", "in": "header", "required": True, "schema": text(), "description": "Quoted revision from draft ETag"})
    if output == "Draft": result["responses"][str(code)]["headers"] = {"ETag": {"description": "Quoted draft revision", "schema": text()}}
    return result
paths = {
 "/healthz": {"get": op("health", "Object")},
 "/v1/capabilities": {"get": op("capabilities", "Object")},
 "/v1/connections/mysql-local/inspect": {"get": op("inspectMySQL", "Probe")},
 "/v1/functions": {"post": op("createFunction", "Function", 201, ref("FunctionRequest"), True)},
 "/v1/functions/{function_id}": {"get": op("getFunction", "Function")},
 "/v1/pipelines/validate": {"post": op("validatePipeline", "Plan", body=ref("Pipeline"))},
 "/v1/pipelines": {"post": op("createPipeline", "Draft", 201, ref("Pipeline"), True), "get": op("listPipelines", "DraftPage")},
 "/v1/pipelines/{pipeline_id}": {"get": op("getPipeline", "Draft"), "put": op("replacePipeline", "Draft", 200, ref("Pipeline"), True, True)},
 "/v1/pipelines/{pipeline_id}/versions": {"post": op("publishVersion", "Version", 201, obj({"draft_revision": text(pattern="^[0-9]+$")}), True)},
 "/v1/pipelines/{pipeline_id}/versions/{version_id}": {"get": op("getVersion", "Version")},
 "/v1/pipelines/{pipeline_id}/instances": {"post": op("createInstance", "Instance", 201, obj({"version_id": nonempty}), True)},
 "/v1/instances/{instance_id}": {"get": op("getInstance", "Instance")},
 "/v1/instances/{instance_id}/checkpoint": {"get": op("getCheckpoint", "Checkpoint")},
 "/v1/instances/{instance_id}/outputs/{output}": {"get": op("getOutputRows", "Rows")},
}
for action in ["pause", "resume"]:
    paths["/v1/instances/{instance_id}/" + action] = {"post": op(action + "Instance", "Instance", 202, obj({}), True)}
paths["/v1/connections/mysql-local/inspect"]["get"]["parameters"] = [{"name": "tables", "in": "query", "required": True, "schema": text(), "example": "shop.orders"}]
import re
for path, item in paths.items():
    for name in re.findall(r"\{([^}]+)\}", path):
        item.setdefault("parameters", []).append({"name": name, "in": "path", "required": True, "schema": nonempty})
write("openapi.json", {"openapi": "3.1.1", "jsonSchemaDialect": "https://json-schema.org/draft/2020-12/schema", "info": {"title": "Local CDC + Lua MVP", "version": "0.1.0", "description": "Implemented loopback-only single-user API, no authentication. Local JSONL/manifest only, not Iceberg/S3. 1 MiB JSON requests; duplicate/unknown keys rejected. Lifecycle commands are asynchronous; inspect instance state."}, "servers": [{"url": "http://127.0.0.1:8080"}], "paths": paths, "components": {"schemas": schemas}})
