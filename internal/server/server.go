// Package server implements the single-user, local-disk MVP control plane.
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	"example.com/etl/internal/localstore"
	"example.com/etl/internal/luaruntime"
	"example.com/etl/internal/model"
	"example.com/etl/internal/mysqlcdc"
	"example.com/etl/pkg/pipeline"
)

type Config struct {
	DataDir          string
	MySQL            mysqlcdc.Connection
	WorkerExecutable string
}
type Function struct {
	ID     string `json:"function_id"`
	Name   string `json:"name"`
	Source string `json:"source"`
	Digest string `json:"digest"`
}
type Draft struct {
	ID         string              `json:"pipeline_id"`
	Revision   uint64              `json:"revision,string"`
	Definition pipeline.Definition `json:"definition"`
}
type Version struct {
	ID            string        `json:"version_id"`
	PipelineID    string        `json:"pipeline_id"`
	DraftRevision uint64        `json:"draft_revision,string"`
	Plan          pipeline.Plan `json:"plan"`
}
type Instance struct {
	ID                  string                    `json:"instance_id"`
	PipelineID          string                    `json:"pipeline_id"`
	VersionID           string                    `json:"version_id"`
	State               string                    `json:"state"`
	DesiredState        string                    `json:"desired_state"`
	LastError           string                    `json:"last_error,omitempty"`
	InitialPosition     model.Position            `json:"initial_position"`
	SourceID            string                    `json:"source_id"`
	ReplicationServerID uint32                    `json:"replication_server_id"`
	Schemas             map[string][]model.Column `json:"schemas"`
}
type idem struct {
	Digest string          `json:"digest"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
	ETag   string          `json:"etag,omitempty"`
}
type catalog struct {
	Functions   map[string]Function `json:"functions"`
	Pipelines   map[string]Draft    `json:"pipelines"`
	Versions    map[string]Version  `json:"versions"`
	Instances   map[string]Instance `json:"instances"`
	Idempotency map[string]idem     `json:"idempotency"`
}
type Server struct {
	mu      sync.Mutex
	cfg     Config
	data    catalog
	store   *localstore.Store
	mux     *http.ServeMux
	root    context.Context
	cancel  context.CancelFunc
	workers map[string]context.CancelFunc
	wg      sync.WaitGroup
	lock    *os.File
	closing bool
}

func Open(parent context.Context, cfg Config) (*Server, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data directory required")
	}
	if cfg.WorkerExecutable == "" {
		return nil, fmt.Errorf("worker executable required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(cfg.DataDir, ".server.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data directory already in use: %w", err)
	}
	store, err := localstore.Open(filepath.Join(cfg.DataDir, "outputs"))
	if err != nil {
		lock.Close()
		return nil, err
	}
	root, cancel := context.WithCancel(parent)
	s := &Server{cfg: cfg, store: store, root: root, cancel: cancel, workers: map[string]context.CancelFunc{}, lock: lock, mux: http.NewServeMux()}
	s.data = catalog{Functions: map[string]Function{}, Pipelines: map[string]Draft{}, Versions: map[string]Version{}, Instances: map[string]Instance{}, Idempotency: map[string]idem{}}
	raw, err := os.ReadFile(filepath.Join(cfg.DataDir, "catalog.json"))
	if err == nil {
		if err = json.Unmarshal(raw, &s.data); err != nil {
			s.Close(context.Background())
			return nil, fmt.Errorf("invalid catalog: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		s.Close(context.Background())
		return nil, err
	}
	if s.data.Functions == nil || s.data.Pipelines == nil || s.data.Versions == nil || s.data.Instances == nil || s.data.Idempotency == nil {
		s.Close(context.Background())
		return nil, fmt.Errorf("incomplete catalog")
	}
	s.routes()
	s.mu.Lock()
	for id, i := range s.data.Instances {
		if i.DesiredState == "RUNNING" {
			s.startLocked(id)
		}
	}
	s.mu.Unlock()
	return s, nil
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method != "GET" && r.Method != "HEAD" && r.Header.Get("Origin") != "" {
		http.Error(w, "browser-origin mutations are disabled", http.StatusForbidden)
		return
	}
	s.mux.ServeHTTP(w, r)
}
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	s.cancel()
	for _, cancel := range s.workers {
		cancel()
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	err := s.store.Close()
	_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	if e := s.lock.Close(); err == nil {
		err = e
	}
	return err
}
func atomicJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".pending-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func (s *Server) saveLocked() error {
	return atomicJSON(filepath.Join(s.cfg.DataDir, "catalog.json"), s.data)
}
func hash(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		fmt.Fprintf(h, "%d:", len(part))
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}
func (s *Server) startLocked(id string) {
	if s.closing {
		return
	}
	if _, running := s.workers[id]; running {
		return
	}
	i, ok := s.data.Instances[id]
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(s.root)
	s.workers[id] = cancel
	i.State = "STARTING"
	i.LastError = ""
	s.data.Instances[id] = i
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := s.run(ctx, id)
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.workers, id)
		i := s.data.Instances[id]
		i.State = "PAUSED"
		if err != nil && ctx.Err() == nil {
			i.DesiredState = "PAUSED"
			i.LastError = err.Error()
		}
		s.data.Instances[id] = i
		if saveErr := s.saveLocked(); saveErr != nil {
			fmt.Fprintf(os.Stderr, "persist instance status: %v\n", saveErr)
		}
	}()
}
func (s *Server) run(ctx context.Context, id string) error {
	s.mu.Lock()
	instance := s.data.Instances[id]
	version := s.data.Versions[instance.VersionID]
	fn, ok := s.data.Functions[version.Plan.Lua().FunctionRef]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("function missing")
	}
	checkpoint, exists, err := s.store.Checkpoint(id)
	if err != nil {
		return err
	}
	start := instance.InitialPosition
	sequence := uint64(0)
	if exists {
		start = checkpoint.Position
		sequence = checkpoint.Sequence
	}
	source := version.Plan.Source()
	connection := s.cfg.MySQL
	connection.ServerID = instance.ReplicationServerID
	cfg := mysqlcdc.Config{Connection: connection, Tables: source.Tables, Start: start, SourceID: instance.SourceID, ExpectedSchemas: instance.Schemas}
	s.mu.Lock()
	i := s.data.Instances[id]
	if i.DesiredState == "RUNNING" {
		i.State = "RUNNING"
	}
	s.data.Instances[id] = i
	err = s.saveLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	worker := luaruntime.Process{Executable: s.cfg.WorkerExecutable}
	return mysqlcdc.Read(ctx, cfg, func(tx model.Transaction) error {
		var records []model.Envelope
		// Stable event IDs survive batching. Fresh Lua state per event prevents
		// batch size from becoming an implicit user-visible state mechanism.
		for offset := 0; offset < len(tx.Events); {
			end := offset
			for end < len(tx.Events) && end-offset < 100 {
				candidate := model.TransformRequest{Source: fn.Source, Events: tx.Events[offset : end+1]}
				raw, _ := json.Marshal(candidate)
				if len(raw) > 900000 {
					break
				}
				end++
			}
			if end == offset {
				return fmt.Errorf("single event exceeds transform request limit")
			}
			callCtx, cancel := context.WithTimeout(ctx, time.Duration(version.Plan.Lua().TimeoutMS)*time.Millisecond)
			result, callErr := worker.Execute(callCtx, model.TransformRequest{Source: fn.Source, Events: tx.Events[offset:end]})
			cancel()
			if callErr != nil {
				return fmt.Errorf("transform failed: %w", callErr)
			}
			if len(result.Results) != end-offset {
				return fmt.Errorf("transform result count mismatch")
			}
			for index, mutations := range result.Results {
				event := tx.Events[offset+index]
				for j, mutation := range mutations {
					records = append(records, model.Envelope{MutationID: hash(id, instance.VersionID, event.ID, fmt.Sprint(j)), EventID: event.ID, Index: j, Mutation: mutation})
				}
			}
			offset = end
		}
		_, err := s.store.Commit(ctx, localstore.CommitRequest{InstanceID: id, Sequence: sequence + 1, TransactionID: tx.ID, VersionID: instance.VersionID, ResumeAfter: tx.ResumeAfter, Outputs: version.Plan.Outputs(), Records: records})
		if err != nil {
			return fmt.Errorf("sink failed: %w", err)
		}
		sequence++
		return nil
	})
}
func sortedDrafts(m map[string]Draft) []Draft {
	out := make([]Draft, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
