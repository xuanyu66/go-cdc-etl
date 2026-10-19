package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"

	"example.com/etl/internal/localstore"
	"example.com/etl/internal/model"
	"example.com/etl/pkg/pipeline"
)

func TestOutputIncludesSnapshotCheckpoint(t *testing.T) {
	s, err := Open(context.Background(), Config{DataDir: t.TempDir(), WorkerExecutable: os.Args[0]})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	raw, err := os.ReadFile("../../examples/pipeline.json")
	if err != nil {
		t.Fatal(err)
	}
	d, err := pipeline.DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := pipeline.Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	s.data.Versions["version-1"] = Version{ID: "version-1", Plan: plan}
	initial := model.Position{File: "mysql-bin.000001", Offset: 4}
	s.data.Instances["instance-1"] = Instance{ID: "instance-1", VersionID: "version-1", InitialPosition: initial}
	get := func() localstore.OutputSnapshot {
		t.Helper()
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequestWithContext(context.Background(), "GET", "/v1/instances/instance-1/outputs/orders", nil))
		if w.Code != 200 {
			t.Fatalf("output response: %d %s", w.Code, w.Body.String())
		}
		var snapshot localstore.OutputSnapshot
		if err := json.Unmarshal(w.Body.Bytes(), &snapshot); err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	before := get()
	if before.Rows == nil || len(before.Rows) != 0 || before.Checkpoint.Sequence != 0 || before.Checkpoint.Position != initial || before.Checkpoint.VersionID != "version-1" {
		t.Fatalf("initial output: %+v", before)
	}
	r := localstore.CommitRequest{InstanceID: "instance-1", Sequence: 1, TransactionID: "tx-1", VersionID: "version-1", ResumeAfter: model.Position{File: initial.File, Offset: 100}, Outputs: plan.Outputs()}
	if _, err := s.store.Commit(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	empty := get()
	if len(empty.Rows) != 0 || empty.Checkpoint.Sequence != 1 || empty.Checkpoint.Position != r.ResumeAfter {
		t.Fatalf("filtered output lost progress: %+v", empty)
	}
	r.Sequence = 2
	r.TransactionID = "tx-2"
	r.ResumeAfter.Offset = 200
	r.Records = []model.Envelope{{MutationID: "m-2", EventID: "e-2", Index: 0, Mutation: model.Mutation{Output: "orders", Operation: "upsert", Key: map[string]any{"order_id": "1"}, Value: map[string]any{"order_id": "1", "customer_id": "7", "total": "24"}}}}
	if _, err := s.store.Commit(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	after := get()
	if len(after.Rows) != 1 || after.Rows[0]["total"] != "24" || after.Checkpoint.Sequence != 2 || after.Checkpoint.TransactionID != "tx-2" || after.Checkpoint.Position != r.ResumeAfter {
		t.Fatalf("committed output: %+v", after)
	}
}

func TestDraftVersionsIdempotencyAndPersistence(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(context.Background(), Config{DataDir: dir, WorkerExecutable: os.Args[0]})
	if err != nil {
		t.Fatal(err)
	}
	s.data.Functions["REPLACE_WITH_FUNCTION_ID"] = Function{ID: "REPLACE_WITH_FUNCTION_ID", Source: "function transform(e) return {} end"}
	request := func(method, path, key, etag string, raw []byte) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequestWithContext(context.Background(), method, path, bytes.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set("If-Match", etag)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	raw, err := os.ReadFile("../../examples/pipeline.json")
	if err != nil {
		t.Fatal(err)
	}
	created := request("POST", "/v1/pipelines", "create", "", raw)
	if created.Code != 201 {
		t.Fatal(created.Body.String())
	}
	var draft Draft
	if err = json.Unmarshal(created.Body.Bytes(), &draft); err != nil {
		t.Fatal(err)
	}
	replay := request("POST", "/v1/pipelines", "create", "", raw)
	if replay.Code != 201 || replay.Body.String() != created.Body.String() {
		t.Fatalf("idempotency failed: %s", replay.Body.String())
	}
	conflict := request("POST", "/v1/pipelines", "create", "", []byte(`{}`))
	if conflict.Code != 409 {
		t.Fatalf("expected conflict, got %d", conflict.Code)
	}
	if w := request("PUT", "/v1/pipelines/"+draft.ID, "missing-condition", "", raw); w.Code != 428 {
		t.Fatalf("missing If-Match: %d", w.Code)
	}
	if w := request("PUT", "/v1/pipelines/"+draft.ID, "stale-condition", `"99"`, raw); w.Code != 412 {
		t.Fatalf("stale If-Match: %d", w.Code)
	}
	published := request("POST", "/v1/pipelines/"+draft.ID+"/versions", "publish", "", []byte(`{"draft_revision":"1"}`))
	if published.Code != 201 {
		t.Fatal(published.Body.String())
	}
	var version Version
	_ = json.Unmarshal(published.Body.Bytes(), &version)
	if w := request("PUT", "/v1/pipelines/"+draft.ID, "edit", `"1"`, raw); w.Code != 200 || w.Header().Get("ETag") != `"2"` {
		t.Fatalf("edit failed: %s", w.Body.String())
	}
	if s.data.Versions[version.ID].DraftRevision != 1 {
		t.Fatal("publishing/edit mutated old version")
	}
	if err = s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, err = Open(context.Background(), Config{DataDir: dir, WorkerExecutable: os.Args[0]})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if w := request("POST", "/v1/pipelines", "create", "", raw); w.Body.String() != created.Body.String() {
		t.Fatalf("idempotency did not survive restart: %s", w.Body.String())
	}
}

func TestHTTPRejectsDuplicateKeysAndBrowserMutations(t *testing.T) {
	s, err := Open(context.Background(), Config{DataDir: t.TempDir(), WorkerExecutable: os.Args[0]})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	r := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/functions", bytes.NewBufferString(`{"name":"a","name":"b","source":"x"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "duplicate")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatalf("duplicate keys accepted: %d %s", w.Code, w.Body.String())
	}
	r = httptest.NewRequestWithContext(context.Background(), "POST", "/v1/functions", bytes.NewBufferString(`{}`))
	r.Header.Set("Origin", "https://example.com")
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("browser mutation accepted: %d", w.Code)
	}
}
