//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"example.com/etl/internal/localstore"
	"example.com/etl/internal/model"
	"example.com/etl/internal/mysqlcdc"
	"example.com/etl/internal/server"
	"example.com/etl/pkg/pipeline"
)

// TestMySQLLuaLocal exercises the public HTTP API against the dedicated Compose
// MySQL, an actual Lua worker subprocess and durable temporary disk output.
// It deliberately does not use t.Parallel: controlled DDL stops active sources.
func TestMySQLLuaLocal(t *testing.T) {
	if os.Getenv("ETL_E2E") != "1" {
		t.Skip("set ETL_E2E=1 and start docker compose up -d --wait mysql")
	}
	addr := os.Getenv("ETL_E2E_ADDR")
	if addr == "" {
		addr = "127.0.0.1:13306"
	}
	if addr != "127.0.0.1:13306" {
		t.Fatal("this test writes through the dedicated Compose container; ETL_E2E_ADDR must be 127.0.0.1:13306")
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, root: root}
	table := fmt.Sprintf("e2e_orders_%d", time.Now().UnixNano())
	timestampTable := fmt.Sprintf("e2e_timestamp_defaults_%d", time.Now().UnixNano())
	generatedTable := fmt.Sprintf("e2e_generated_column_%d", time.Now().UnixNano())
	h.sql("CREATE TABLE shop." + table + " (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT NOT NULL, amount DECIMAL(18,2) NOT NULL, status VARCHAR(32) NOT NULL) ENGINE=InnoDB")
	defer h.sql("DROP TABLE IF EXISTS shop." + table)
	h.sql("CREATE TABLE shop." + timestampTable + " (id BIGINT PRIMARY KEY, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) ENGINE=InnoDB")
	defer h.sql("DROP TABLE IF EXISTS shop." + timestampTable)
	h.sql("CREATE TABLE shop." + generatedTable + " (id INT PRIMARY KEY, derived INT GENERATED ALWAYS AS (id + 1) STORED) ENGINE=InnoDB")
	defer h.sql("DROP TABLE IF EXISTS shop." + generatedTable)
	h.sql("INSERT INTO shop." + table + " VALUES (99, 7, 9.99, 'paid')")
	worker := filepath.Join(t.TempDir(), "etl")
	build := exec.Command("go", "build", "-o", worker, "./cmd/etl")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}
	cfg := server.Config{DataDir: t.TempDir(), WorkerExecutable: worker, MySQL: mysqlcdc.Connection{Addr: addr, User: "etl", Password: "etl-demo-password", ServerID: 80301}}
	h.open(cfg)
	defer h.close()
	var timestampProbe mysqlcdc.ProbeResult
	h.request(http.MethodGet, "/v1/connections/mysql-local/inspect?tables=shop."+timestampTable, "", nil, http.StatusOK, &timestampProbe)
	if len(timestampProbe.Tables["shop."+timestampTable]) != 3 {
		t.Fatalf("ordinary TIMESTAMP defaults were not accepted by inspect: %+v", timestampProbe)
	}
	h.request(http.MethodGet, "/v1/connections/mysql-local/inspect?tables=shop."+generatedTable, "", nil, http.StatusUnprocessableEntity, nil)

	const source = `function transform(event)
  local old = event.before
  local row = event.after
  if event.op == "delete" then
    return {{output="orders", op="delete", key={id=old.id}}}
  end
  if row.status ~= "paid" then
    if event.op == "update" and old.status == "paid" then
      return {{output="orders", op="delete", key={id=old.id}}}
    end
    return {}
  end
  local total = etl.decimal_mul(row.amount, "2")
  return {{output="orders", op="upsert", key={id=row.id}, value={id=row.id, customer=string.upper("customer-" .. row.customer_id), total=total}}}
end`
	instance := h.create(table, source)
	h.state(instance.ID, "RUNNING", false)
	if rows := h.rows(instance.ID); len(rows) != 0 {
		t.Fatalf("incremental source exported initial rows: %#v", rows)
	}

	// DDL for an unrelated table must not pause this fixed-table pipeline or
	// strand its checkpoint. This also covers manual demo + E2E coexistence.
	unrelated := table + "_scratch"
	beforeDDL := h.checkpoint(instance.ID)
	h.sql("CREATE TABLE shop." + unrelated + " (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
	h.sql("ALTER TABLE shop." + unrelated + " ADD COLUMN note VARCHAR(8)")
	h.sql("RENAME TABLE shop." + unrelated + " TO shop." + unrelated + "_renamed")
	h.sql("DROP TABLE shop." + unrelated + "_renamed")
	h.eventually("unrelated DDL checkpoint", func() bool {
		return h.checkpoint(instance.ID).Sequence >= beforeDDL.Sequence+4
	})
	h.state(instance.ID, "RUNNING", false)

	// A multi-row transaction is visible only after its whole output commits.
	h.sql("START TRANSACTION; INSERT INTO shop." + table + " VALUES (9007199254740993, 7, 12.34, 'paid'), (2, 8, 8.75, 'paid'), (3, 9, 1.00, 'draft'); COMMIT")
	h.wantRows(instance.ID, []map[string]any{{"id": "9007199254740993", "customer": "CUSTOMER-7", "total": "24.68"}, {"id": "2", "customer": "CUSTOMER-8", "total": "17.5"}})
	cp := h.checkpoint(instance.ID)
	if cp.Sequence == 0 || cp.TransactionID == "" || cp.Position.File == "" {
		t.Fatalf("missing durable checkpoint: %+v", cp)
	}
	h.sql("UPDATE shop." + table + " SET amount=15.55 WHERE id=9007199254740993")
	h.wantRows(instance.ID, []map[string]any{{"id": "9007199254740993", "customer": "CUSTOMER-7", "total": "31.1"}, {"id": "2", "customer": "CUSTOMER-8", "total": "17.5"}})
	h.sql("DELETE FROM shop." + table + " WHERE id=2")
	h.wantRows(instance.ID, []map[string]any{{"id": "9007199254740993", "customer": "CUSTOMER-7", "total": "31.1"}})
	h.sql("UPDATE shop." + table + " SET status='cancelled' WHERE id=9007199254740993")
	h.wantRows(instance.ID, []map[string]any{})
	beforeFiltered := h.checkpoint(instance.ID)
	h.sql("INSERT INTO shop." + table + " VALUES (4, 4, 1.00, 'draft')")
	h.eventually("filtered transaction checkpoint", func() bool { return h.checkpoint(instance.ID).Sequence > beforeFiltered.Sequence })
	if rows := h.rows(instance.ID); len(rows) != 0 {
		t.Fatalf("filtered rows leaked: %#v", rows)
	}

	h.pause(instance.ID)
	paused := h.checkpoint(instance.ID)
	h.sql("INSERT INTO shop." + table + " VALUES (5, 5, 10.01, 'paid')")
	if got := h.checkpoint(instance.ID); got != paused {
		t.Fatalf("paused checkpoint advanced: %+v -> %+v", paused, got)
	}
	h.post("/v1/instances/"+instance.ID+"/resume", map[string]any{}, http.StatusAccepted, nil)
	h.wantRows(instance.ID, []map[string]any{{"id": "5", "customer": "CUSTOMER-5", "total": "20.02"}})

	// Restart on the same disk, with a committed source position. Events written
	// while offline must be replayed; prior committed files must not be duplicated.
	h.close()
	filesBefore := committedFiles(t, cfg.DataDir, instance.ID)
	h.sql("INSERT INTO shop." + table + " VALUES (6, 6, 0.10, 'paid')")
	h.open(cfg)
	h.wantRows(instance.ID, []map[string]any{{"id": "5", "customer": "CUSTOMER-5", "total": "20.02"}, {"id": "6", "customer": "CUSTOMER-6", "total": "0.2"}})
	h.pause(instance.ID)
	filesAfter := committedFiles(t, cfg.DataDir, instance.ID)
	if len(filesAfter) < len(filesBefore)+1 {
		t.Fatalf("restart lost committed transaction files: %d -> %d", len(filesBefore), len(filesAfter))
	}
	for name, content := range filesBefore {
		if filesAfter[name] != content {
			t.Fatalf("restart changed prior committed file %s", name)
		}
	}

	for _, tc := range []struct{ name, source, errorText string }{
		{"lua_error", `function transform(event) error("intentional E2E failure") end`, "intentional E2E failure"},
		{"schema_error", `function transform(event) return {{output="orders",op="upsert",key={id=event.after.id},value={id=event.after.id,customer="x",total="not-a-decimal"}}} end`, "sink failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := h.t
			h.t = t
			defer func() { h.t = previous }()
			bad := h.create(table, tc.source)
			h.state(bad.ID, "RUNNING", false)
			before := h.checkpoint(bad.ID)
			h.sql("INSERT INTO shop." + table + " VALUES (" + map[string]string{"lua_error": "7", "schema_error": "8"}[tc.name] + ", 7, 1.00, 'paid')")
			status := h.state(bad.ID, "PAUSED", true)
			if !strings.Contains(status.LastError, tc.errorText) {
				t.Fatalf("unexpected failure: %s", status.LastError)
			}
			if after := h.checkpoint(bad.ID); after != before {
				t.Fatalf("failed transform advanced checkpoint: %+v -> %+v", before, after)
			}
			if rows := h.rows(bad.ID); len(rows) != 0 {
				t.Fatalf("failed transform produced output: %#v", rows)
			}
		})
	}

	t.Run("controlled_ddl", func(t *testing.T) {
		previous := h.t
		h.t = t
		defer func() { h.t = previous }()
		ddl := h.create(table, source)
		h.state(ddl.ID, "RUNNING", false)
		before := h.checkpoint(ddl.ID)
		h.sql("ALTER TABLE shop." + table + " ADD COLUMN note VARCHAR(32)")
		status := h.state(ddl.ID, "PAUSED", true)
		if !strings.Contains(status.LastError, "DDL") {
			t.Fatalf("DDL did not report controlled stop: %s", status.LastError)
		}
		if after := h.checkpoint(ddl.ID); after != before {
			t.Fatalf("DDL advanced checkpoint: %+v -> %+v", before, after)
		}
	})
}

// TestControlPlaneAndMultiOutputE2E exercises the implemented HTTP resource
// lifecycle and multiple outputs through a real MySQL binlog transaction.
func TestControlPlaneAndMultiOutputE2E(t *testing.T) {
	if os.Getenv("ETL_E2E") != "1" {
		t.Skip("set ETL_E2E=1 and start docker compose up -d --wait mysql")
	}
	addr := os.Getenv("ETL_E2E_ADDR")
	if addr == "" {
		addr = "127.0.0.1:13306"
	}
	if addr != "127.0.0.1:13306" {
		t.Fatal("ETL_E2E_ADDR must be the dedicated Compose MySQL at 127.0.0.1:13306")
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, root: root}
	table := fmt.Sprintf("e2e_control_%d", time.Now().UnixNano())
	h.sql("CREATE TABLE shop." + table + " (id BIGINT UNSIGNED PRIMARY KEY, label VARCHAR(64) NOT NULL) ENGINE=InnoDB")
	defer h.sql("DROP TABLE IF EXISTS shop." + table)
	worker := filepath.Join(t.TempDir(), "etl")
	build := exec.Command("go", "build", "-o", worker, "./cmd/etl")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}
	cfg := server.Config{DataDir: t.TempDir(), WorkerExecutable: worker, MySQL: mysqlcdc.Connection{Addr: addr, User: "etl", Password: "etl-demo-password", ServerID: 80311}}
	h.open(cfg)
	defer h.close()

	h.request(http.MethodGet, "/healthz", "", nil, http.StatusOK, nil)
	var caps map[string]any
	h.request(http.MethodGet, "/v1/capabilities", "", nil, http.StatusOK, &caps)
	if caps["source"] != "mysql-row-full" || caps["incremental_only"] != true {
		t.Fatalf("unexpected capabilities: %#v", caps)
	}
	var inspected mysqlcdc.ProbeResult
	h.request(http.MethodGet, "/v1/connections/mysql-local/inspect?tables=shop."+table, "", nil, http.StatusOK, &inspected)
	if inspected.Position.File == "" || len(inspected.Tables["shop."+table]) != 2 {
		t.Fatalf("inspect omitted position or schema: %+v", inspected)
	}
	h.request(http.MethodGet, "/v1/connections/mysql-local/inspect", "", nil, http.StatusBadRequest, nil)
	h.request(http.MethodGet, "/v1/functions/missing", "", nil, http.StatusNotFound, nil)
	h.request(http.MethodPost, "/v1/functions", "bad-function", map[string]string{"name": "bad", "source": "function nope() end"}, http.StatusUnprocessableEntity, nil)

	functionSource := `function transform(event)
  local row = event.after
  if event.op == "delete" then row = event.before end
  if event.op == "delete" then
    return {
      {output="primary", op="delete", key={id=row.id}},
      {output="audit", op="delete", key={id=row.id}}
    }
  end
  return {
    {output="primary", op="upsert", key={id=row.id}, value={id=row.id, label=string.upper(row.label)}},
    {output="audit", op="upsert", key={id=row.id}, value={id=row.id, label=row.label}}
  }
end`
	var fn server.Function
	firstFn := h.request(http.MethodPost, "/v1/functions", "fn-create", map[string]string{"name": "transform", "source": functionSource}, http.StatusCreated, &fn)
	secondFn := h.request(http.MethodPost, "/v1/functions", "fn-create", map[string]string{"name": "transform", "source": functionSource}, http.StatusCreated, nil)
	if !bytes.Equal(firstFn, secondFn) {
		t.Fatal("function creation replay was not idempotent")
	}
	h.request(http.MethodPost, "/v1/functions", "fn-create", map[string]string{"name": "transform", "source": functionSource + "\n"}, http.StatusConflict, nil)
	var loadedFn server.Function
	h.request(http.MethodGet, "/v1/functions/"+fn.ID, "", nil, http.StatusOK, &loadedFn)
	if loadedFn.Digest != fn.Digest {
		t.Fatalf("function digest changed: %+v", loadedFn)
	}

	definition := multiOutputDefinition(table, fn.ID, "e2e-control")
	h.request(http.MethodPost, "/v1/pipelines/validate", "", definition, http.StatusOK, nil)
	invalid := definition
	invalid.Spec.Edges = invalid.Spec.Edges[:2]
	h.request(http.MethodPost, "/v1/pipelines/validate", "", invalid, http.StatusUnprocessableEntity, nil)
	unknownFunction := multiOutputDefinition(table, fn.ID, "e2e-control")
	unknownFunction.Spec.Nodes[1].Lua = &pipeline.LuaConfig{FunctionRef: "fn_missing", TimeoutMS: 1000}
	h.request(http.MethodPost, "/v1/pipelines/validate", "", unknownFunction, http.StatusUnprocessableEntity, nil)
	// The JSON API rejects duplicate keys and unknown fields before decoding.
	h.requestRaw(http.MethodPost, "/v1/pipelines/validate", "", []byte(`{"api_version":"v1","api_version":"v1"}`), http.StatusBadRequest)
	unknownField := []byte(`{"api_version":"v1","kind":"Pipeline","metadata":{"name":"x"},"spec":{"nodes":[],"edges":[],"extra":true}}`)
	h.requestRaw(http.MethodPost, "/v1/pipelines/validate", "", unknownField, http.StatusUnprocessableEntity)

	var draft server.Draft
	firstDraft := h.request(http.MethodPost, "/v1/pipelines", "pipeline-create", definition, http.StatusCreated, &draft)
	secondDraft := h.request(http.MethodPost, "/v1/pipelines", "pipeline-create", definition, http.StatusCreated, nil)
	if !bytes.Equal(firstDraft, secondDraft) {
		t.Fatal("pipeline creation replay was not idempotent")
	}
	h.request(http.MethodPost, "/v1/pipelines", "pipeline-create", multiOutputDefinition(table, fn.ID, "e2e-different"), http.StatusConflict, nil)
	var listed struct {
		Items []server.Draft `json:"items"`
	}
	h.request(http.MethodGet, "/v1/pipelines", "", nil, http.StatusOK, &listed)
	if len(listed.Items) != 1 || listed.Items[0].ID != draft.ID {
		t.Fatalf("pipeline listing mismatch: %+v", listed)
	}
	var loadedDraft server.Draft
	h.request(http.MethodGet, "/v1/pipelines/"+draft.ID, "", nil, http.StatusOK, &loadedDraft)
	if loadedDraft.Revision != 1 {
		t.Fatalf("draft revision = %d, want 1", loadedDraft.Revision)
	}
	updated := multiOutputDefinition(table, fn.ID, "e2e-control-edited")
	var changed server.Draft
	h.requestWithHeaders(http.MethodPut, "/v1/pipelines/"+draft.ID, "update-draft", `"1"`, updated, http.StatusOK, &changed)
	if changed.Revision != 2 {
		t.Fatalf("draft revision after update = %d, want 2", changed.Revision)
	}
	h.requestWithHeaders(http.MethodPut, "/v1/pipelines/"+draft.ID, "no-if-match", "", updated, http.StatusPreconditionRequired, nil)
	h.requestWithHeaders(http.MethodPut, "/v1/pipelines/"+draft.ID, "stale-if-match", `"1"`, updated, http.StatusPreconditionFailed, nil)
	h.request(http.MethodPost, "/v1/pipelines/"+draft.ID+"/versions", "wrong-revision", map[string]string{"draft_revision": "1"}, http.StatusConflict, nil)
	var version server.Version
	h.post("/v1/pipelines/"+draft.ID+"/versions", map[string]string{"draft_revision": "2"}, http.StatusCreated, &version)
	var loadedVersion server.Version
	h.request(http.MethodGet, "/v1/pipelines/"+draft.ID+"/versions/"+version.ID, "", nil, http.StatusOK, &loadedVersion)
	if loadedVersion.DraftRevision != 2 || len(loadedVersion.Plan.RequiredSinks) != 2 {
		t.Fatalf("published version mismatch: %+v", loadedVersion)
	}
	h.request(http.MethodGet, "/v1/pipelines/"+draft.ID+"/versions/missing", "", nil, http.StatusNotFound, nil)

	createBody := map[string]string{"version_id": version.ID}
	var instance server.Instance
	created := h.request(http.MethodPost, "/v1/pipelines/"+draft.ID+"/instances", "instance-create", createBody, http.StatusCreated, &instance)
	var replay server.Instance
	if repeated := h.request(http.MethodPost, "/v1/pipelines/"+draft.ID+"/instances", "instance-create", createBody, http.StatusCreated, &replay); !bytes.Equal(created, repeated) || replay.ID != instance.ID {
		t.Fatal("instance creation replay was not idempotent")
	}
	h.post("/v1/pipelines/"+draft.ID+"/instances", createBody, http.StatusConflict, nil)
	h.state(instance.ID, "RUNNING", false)
	h.request(http.MethodGet, "/v1/instances/missing", "", nil, http.StatusNotFound, nil)
	h.request(http.MethodGet, "/v1/instances/"+instance.ID+"/outputs/missing", "", nil, http.StatusNotFound, nil)
	initial := h.checkpoint(instance.ID)
	if initial.Position.File == "" || initial.VersionID != version.ID {
		t.Fatalf("initial checkpoint missing source position/version: %+v", initial)
	}

	h.sql("INSERT INTO shop." + table + " VALUES (42, 'hello')")
	h.eventually("both output projections", func() bool {
		primary, audit := h.outputRows(instance.ID, "primary"), h.outputRows(instance.ID, "audit")
		return len(primary) == 1 && len(audit) == 1 && primary[0]["label"] == "HELLO" && audit[0]["label"] == "hello"
	})
	h.sql("UPDATE shop." + table + " SET label='world' WHERE id=42")
	h.eventually("both output upserts", func() bool {
		primary, audit := h.outputRows(instance.ID, "primary"), h.outputRows(instance.ID, "audit")
		return len(primary) == 1 && len(audit) == 1 && primary[0]["label"] == "WORLD" && audit[0]["label"] == "world"
	})
	h.sql("DELETE FROM shop." + table + " WHERE id=42")
	h.eventually("both output deletes", func() bool {
		return len(h.outputRows(instance.ID, "primary")) == 0 && len(h.outputRows(instance.ID, "audit")) == 0
	})
	h.pause(instance.ID)
	h.post("/v1/instances/"+instance.ID+"/resume", map[string]any{}, http.StatusAccepted, nil)
	h.state(instance.ID, "RUNNING", false)
	h.pause(instance.ID)
}

func multiOutputDefinition(table, functionID, name string) pipeline.Definition {
	fields := []model.Column{{Name: "id", Type: "uint64", Required: true}, {Name: "label", Type: "string", Required: true}}
	return pipeline.Definition{APIVersion: "v1", Kind: "Pipeline", Metadata: pipeline.Metadata{Name: name}, Spec: pipeline.Spec{
		Nodes: []pipeline.Node{
			{ID: "source", Type: pipeline.SourceMySQL, MySQL: &pipeline.MySQLConfig{ConnectionRef: "mysql-local", Tables: []string{"shop." + table}, Start: pipeline.Start{Mode: "current"}}},
			{ID: "transform", Type: pipeline.TransformLua, Lua: &pipeline.LuaConfig{FunctionRef: functionID, TimeoutMS: 1000}},
			{ID: "primary", Type: pipeline.SinkLocal, Local: &pipeline.LocalConfig{Output: "primary", KeyFields: []string{"id"}, Fields: fields}},
			{ID: "audit", Type: pipeline.SinkLocal, Local: &pipeline.LocalConfig{Output: "audit", KeyFields: []string{"id"}, Fields: fields}},
		}, Edges: []pipeline.Edge{{From: "source", To: "transform"}, {From: "transform", To: "primary"}, {From: "transform", To: "audit"}},
	}}
}

type harness struct {
	t       *testing.T
	root    string
	service *server.Server
	http    *httptest.Server
	counter uint64
}

func (h *harness) sql(query string) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "exec", "-T", "mysql", "mysql", "-uroot", "-petl-demo-root", "--batch", "--execute", query)
	cmd.Dir = h.root
	if out, err := cmd.CombinedOutput(); err != nil {
		h.t.Fatalf("fixture SQL failed: %v\n%s", err, out)
	}
}

func (h *harness) open(cfg server.Config) {
	h.t.Helper()
	s, err := server.Open(context.Background(), cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	h.service, h.http = s, httptest.NewServer(s)
}

func (h *harness) close() {
	if h.service == nil {
		return
	}
	h.http.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.service.Close(ctx); err != nil {
		h.t.Errorf("close service: %v", err)
	}
	h.service, h.http = nil, nil
}

func (h *harness) request(method, path, key string, body any, code int, result any) []byte {
	return h.requestWithHeaders(method, path, key, "", body, code, result)
}

func (h *harness) requestWithHeaders(method, path, key, ifMatch string, body any, code int, result any) []byte {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, h.http.URL+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	if res.StatusCode != code {
		h.t.Fatalf("%s %s: status %d, want %d: %s", method, path, res.StatusCode, code, raw)
	}
	if result != nil {
		if err := json.Unmarshal(raw, result); err != nil {
			h.t.Fatalf("decode %s: %v: %s", path, err, raw)
		}
	}
	return raw
}

func (h *harness) requestRaw(method, path, key string, body []byte, code int) []byte {
	h.t.Helper()
	req, err := http.NewRequest(method, h.http.URL+path, bytes.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	if res.StatusCode != code {
		h.t.Fatalf("%s %s: status %d, want %d: %s", method, path, res.StatusCode, code, raw)
	}
	return raw
}

func (h *harness) post(path string, body any, code int, result any) {
	h.counter++
	h.request(http.MethodPost, path, fmt.Sprintf("e2e-%d", h.counter), body, code, result)
}

func (h *harness) create(table, source string) server.Instance {
	h.t.Helper()
	var fn server.Function
	h.counter++
	key := fmt.Sprintf("function-%d", h.counter)
	body := map[string]string{"name": "e2e-transform", "source": source}
	first := h.request(http.MethodPost, "/v1/functions", key, body, http.StatusCreated, &fn)
	second := h.request(http.MethodPost, "/v1/functions", key, body, http.StatusCreated, nil)
	if !bytes.Equal(first, second) {
		h.t.Fatal("idempotent function creation changed response")
	}
	d := pipeline.Definition{APIVersion: "v1", Kind: "Pipeline", Metadata: pipeline.Metadata{Name: fmt.Sprintf("e2e-orders-%d", h.counter)}, Spec: pipeline.Spec{
		Nodes: []pipeline.Node{
			{ID: "source", Type: pipeline.SourceMySQL, MySQL: &pipeline.MySQLConfig{ConnectionRef: "mysql-local", Tables: []string{"shop." + table}, Start: pipeline.Start{Mode: "current"}}},
			{ID: "transform", Type: pipeline.TransformLua, Lua: &pipeline.LuaConfig{FunctionRef: fn.ID, TimeoutMS: 2000}},
			{ID: "sink", Type: pipeline.SinkLocal, Local: &pipeline.LocalConfig{Output: "orders", KeyFields: []string{"id"}, Fields: []model.Column{{Name: "id", Type: "uint64", Required: true}, {Name: "customer", Type: "string", Required: true}, {Name: "total", Type: "decimal(18,2)", Required: true}}}},
		}, Edges: []pipeline.Edge{{From: "source", To: "transform"}, {From: "transform", To: "sink"}},
	}}
	var draft server.Draft
	h.post("/v1/pipelines", d, http.StatusCreated, &draft)
	var version server.Version
	h.post("/v1/pipelines/"+draft.ID+"/versions", map[string]string{"draft_revision": fmt.Sprint(draft.Revision)}, http.StatusCreated, &version)
	var instance server.Instance
	h.post("/v1/pipelines/"+draft.ID+"/instances", map[string]string{"version_id": version.ID}, http.StatusCreated, &instance)
	return instance
}

func (h *harness) checkpoint(id string) localstore.Checkpoint {
	var cp localstore.Checkpoint
	h.request(http.MethodGet, "/v1/instances/"+id+"/checkpoint", "", nil, http.StatusOK, &cp)
	return cp
}

func (h *harness) rows(id string) []map[string]any {
	return h.outputRows(id, "orders")
}

func (h *harness) outputRows(id, output string) []map[string]any {
	h.t.Helper()
	var res struct {
		Rows       []map[string]any      `json:"rows"`
		Checkpoint localstore.Checkpoint `json:"checkpoint"`
	}
	h.request(http.MethodGet, "/v1/instances/"+id+"/outputs/"+output, "", nil, http.StatusOK, &res)
	if res.Checkpoint.Position.File == "" || res.Checkpoint.VersionID == "" {
		h.t.Fatalf("output response omitted its applied checkpoint: %+v", res)
	}
	return res.Rows
}

func (h *harness) eventually(label string, f func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", label)
}

func (h *harness) wantRows(id string, want []map[string]any) {
	h.t.Helper()
	var got []map[string]any
	h.eventually("expected output rows", func() bool {
		got = h.rows(id)
		var status server.Instance
		h.request(http.MethodGet, "/v1/instances/"+id, "", nil, http.StatusOK, &status)
		if status.LastError != "" {
			h.t.Fatalf("pipeline failed while waiting for rows: %s; got %#v, want %#v", status.LastError, got, want)
		}
		if len(got) != len(want) {
			return false
		}
		byID := map[string]map[string]any{}
		for _, row := range got {
			byID[row["id"].(string)] = row
		}
		for _, row := range want {
			if !reflect.DeepEqual(byID[row["id"].(string)], row) {
				return false
			}
		}
		return true
	})
}

func (h *harness) state(id, state string, failed bool) server.Instance {
	h.t.Helper()
	var status server.Instance
	h.eventually("instance "+state, func() bool {
		h.request(http.MethodGet, "/v1/instances/"+id, "", nil, http.StatusOK, &status)
		if state == "RUNNING" && status.State == "PAUSED" && status.LastError != "" {
			h.t.Fatalf("pipeline failed to start: %s", status.LastError)
		}
		return status.State == state && (!failed || status.LastError != "")
	})
	return status
}

func (h *harness) pause(id string) {
	h.post("/v1/instances/"+id+"/pause", map[string]any{}, http.StatusAccepted, nil)
	h.state(id, "PAUSED", false)
}

func committedFiles(t *testing.T, dir, id string) map[string]string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, "outputs", id, "*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, name := range names {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.Base(name)] = string(b)
	}
	return files
}
