package localstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"example.com/etl/internal/model"
)

func request() CommitRequest {
	return CommitRequest{
		InstanceID: "instance-1", Sequence: 1, TransactionID: "binlog:123", VersionID: "v1", ResumeAfter: model.Position{File: "mysql-bin.000001", Offset: 123},
		Outputs: []model.Output{{Name: "orders", KeyFields: []string{"id"}, Fields: []model.Column{{Name: "id", Type: "uint64", Required: true}, {Name: "total", Type: "decimal(18,2)", Required: true}}}, {Name: "audit", KeyFields: []string{"id"}, Fields: []model.Column{{Name: "id", Type: "string", Required: true}}}},
		Records: []model.Envelope{{MutationID: "m1", EventID: "e1", Index: 0, Mutation: model.Mutation{Output: "orders", Operation: "upsert", Key: map[string]any{"id": "18446744073709551615"}, Value: map[string]any{"id": "18446744073709551615", "total": "1234567890123456.78"}}}},
	}
}
func openTest(t *testing.T, root string) *Store {
	t.Helper()
	s, e := Open(root)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestCommitRecoveryAndDelete(t *testing.T) {
	root := t.TempDir()
	s := openTest(t, root)
	r := request()
	cp, e := s.Commit(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.Commit(context.Background(), r)
	if e != nil || again != cp {
		t.Fatalf("replay: %+v %v", again, e)
	}
	if _, e = Open(root); e == nil {
		t.Fatal("concurrent open accepted")
	}
	s.Close()
	s = openTest(t, root)
	got, ok, e := s.Checkpoint(r.InstanceID)
	if e != nil || !ok || got != cp {
		t.Fatalf("checkpoint: %+v %v %v", got, ok, e)
	}
	rows, e := s.Rows(r.InstanceID, "orders")
	if e != nil || len(rows) != 1 || rows[0]["id"] != "18446744073709551615" || rows[0]["total"] != "1234567890123456.78" {
		t.Fatalf("lossy rows: %#v %v", rows, e)
	}
	audit, e := s.Rows(r.InstanceID, "audit")
	if e != nil || len(audit) != 0 {
		t.Fatal(audit, e)
	}
	r.Sequence = 2
	r.TransactionID = "binlog:456"
	r.ResumeAfter.Offset = 456
	r.Records[0].MutationID = "m2"
	r.Records[0].Mutation.Operation = "delete"
	r.Records[0].Mutation.Value = nil
	if _, e = s.Commit(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	rows, e = s.Rows(r.InstanceID, "orders")
	if e != nil || len(rows) != 0 {
		t.Fatal(rows, e)
	}
	commits, e := s.Commits(r.InstanceID)
	if e != nil || len(commits) != 2 {
		t.Fatal(commits, e)
	}
}
func TestValidationAndNoCheckpoint(t *testing.T) {
	tests := map[string]func(*CommitRequest){
		"undeclared output": func(r *CommitRequest) { r.Records[0].Mutation.Output = "other" },
		"missing required":  func(r *CommitRequest) { delete(r.Records[0].Mutation.Value, "total") },
		"unknown field":     func(r *CommitRequest) { r.Records[0].Mutation.Value["other"] = "x" },
		"key mismatch":      func(r *CommitRequest) { r.Records[0].Mutation.Key["id"] = "2" },
		"key extra":         func(r *CommitRequest) { r.Records[0].Mutation.Key["x"] = "2" },
		"decimal overflow":  func(r *CommitRequest) { r.Records[0].Mutation.Value["total"] = "12345678901234567.89" },
		"decimal scale":     func(r *CommitRequest) { r.Records[0].Mutation.Value["total"] = "1.234" },
		"lossy integer":     func(r *CommitRequest) { r.Records[0].Mutation.Value["id"] = float64(1) },
		"delete value":      func(r *CommitRequest) { r.Records[0].Mutation.Operation = "delete" },
		"sequence gap":      func(r *CommitRequest) { r.Sequence = 2 },
		"path traversal":    func(r *CommitRequest) { r.InstanceID = "../escape" },
		"output traversal":  func(r *CommitRequest) { r.Outputs[0].Name = "../escape" },
		"no outputs":        func(r *CommitRequest) { r.Outputs = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			s := openTest(t, t.TempDir())
			r := request()
			mutate(&r)
			if _, e := s.Commit(context.Background(), r); e == nil {
				t.Fatal("invalid request accepted")
			}
			_, ok, e := s.Checkpoint("instance-1")
			if e != nil || ok {
				t.Fatalf("checkpoint advanced: %v %v", ok, e)
			}
		})
	}
}
func TestConflictAndCrashBeforePublish(t *testing.T) {
	root := t.TempDir()
	s := openTest(t, root)
	r := request()
	s.beforePublish = func() error { return errors.New("simulated crash") }
	if _, e := s.Commit(context.Background(), r); e == nil {
		t.Fatal("expected failure")
	}
	_, ok, e := s.Checkpoint(r.InstanceID)
	if e != nil || ok {
		t.Fatal("uncommitted checkpoint", ok, e)
	}
	rows, e := s.Rows(r.InstanceID, "orders")
	if e != nil || len(rows) != 0 {
		t.Fatal("uncommitted rows visible", rows, e)
	}
	entries, e := os.ReadDir(filepath.Join(root, r.InstanceID))
	if e != nil || len(entries) != 2 {
		t.Fatal("expected both output data files", entries, e)
	}
	s.Close()
	s = openTest(t, root)
	if _, e = s.Commit(context.Background(), r); e != nil {
		t.Fatal("retry orphan files", e)
	}
	r.Records[0].Mutation.Value["total"] = "2.00"
	if _, e = s.Commit(context.Background(), r); e == nil || !strings.Contains(e.Error(), "CONTENT_CONFLICT") {
		t.Fatal("expected content conflict", e)
	}
	cp, ok, e := s.Checkpoint(r.InstanceID)
	if e != nil || !ok || cp.Sequence != 1 {
		t.Fatal(cp, ok, e)
	}
}
func TestCancelledAndEmptyTransaction(t *testing.T) {
	s := openTest(t, t.TempDir())
	r := request()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.Commit(ctx, r); e == nil {
		t.Fatal("cancel ignored")
	}
	r.Records = nil
	if _, e := s.Commit(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	cp, ok, e := s.Checkpoint(r.InstanceID)
	if e != nil || !ok || cp.Sequence != 1 {
		t.Fatal(cp, ok, e)
	}
}
func TestSymlinkRejected(t *testing.T) {
	s := openTest(t, t.TempDir())
	if e := os.Symlink(t.TempDir(), filepath.Join(s.root, "instance-1")); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Commit(context.Background(), request()); e == nil {
		t.Fatal("symlink accepted")
	}
}
