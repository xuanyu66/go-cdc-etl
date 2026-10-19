package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"example.com/etl/internal/luaruntime"
	"example.com/etl/internal/model"
	"example.com/etl/internal/mysqlcdc"
	"example.com/etl/pkg/pipeline"
)

type apiError struct {
	status        int
	code, message string
}

func (e *apiError) Error() string                 { return e.message }
func fail(status int, code, message string) error { return &apiError{status, code, message} }
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func problem(w http.ResponseWriter, err error) {
	status, code := 422, "INVALID_REQUEST"
	var ae *apiError
	if errors.As(err, &ae) {
		status, code = ae.status, ae.code
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	body := map[string]any{"type": "urn:etl:problem:" + strings.ToLower(code), "title": http.StatusText(status), "status": status, "code": code, "detail": err.Error()}
	var ve *pipeline.ValidationError
	if errors.As(err, &ve) {
		body["errors"] = ve.Violations
	}
	_ = json.NewEncoder(w).Encode(body)
}
func bodyBytes(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	media := strings.Split(r.Header.Get("Content-Type"), ";")[0]
	if media != "application/json" {
		return nil, fail(415, "CONTENT_TYPE", "Content-Type must be application/json")
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fail(413, "BODY_TOO_LARGE", "request body exceeds 1 MiB")
	}
	if err := pipeline.ValidateJSON(raw); err != nil {
		return nil, fail(400, "INVALID_JSON", err.Error())
	}
	return raw, nil
}
func decode(raw []byte, out any) error {
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fail(400, "INVALID_JSON", err.Error())
	}
	var tail any
	if d.Decode(&tail) != io.EOF {
		return fail(400, "INVALID_JSON", "expected one JSON document")
	}
	return nil
}

type mutation func([]byte) (any, int, string, func(), error)

func (s *Server) mutate(w http.ResponseWriter, r *http.Request, fn mutation) {
	raw, err := bodyBytes(w, r)
	if err != nil {
		problem(w, err)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 128 || strings.TrimSpace(key) != key {
		problem(w, fail(400, "IDEMPOTENCY_KEY_REQUIRED", "provide an Idempotency-Key of 1..128 characters"))
		return
	}
	scope := r.Method + " " + r.URL.Path + " " + key
	digest := hash(string(raw), r.Header.Get("If-Match"))
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		problem(w, fail(503, "SHUTTING_DOWN", "server is closing"))
		return
	}
	if previous, ok := s.data.Idempotency[scope]; ok {
		s.mu.Unlock()
		if previous.Digest != digest {
			problem(w, fail(409, "IDEMPOTENCY_CONFLICT", "key was used with a different body or precondition"))
			return
		}
		if previous.ETag != "" {
			w.Header().Set("ETag", previous.ETag)
		}
		writeJSON(w, previous.Status, previous.Body)
		return
	}
	snapshot, _ := json.Marshal(s.data)
	value, status, etag, after, err := fn(raw)
	if err == nil {
		var response []byte
		response, err = json.Marshal(value)
		if err == nil {
			s.data.Idempotency[scope] = idem{Digest: digest, Status: status, Body: response, ETag: etag}
			err = s.saveLocked()
		}
	}
	if err != nil {
		_ = json.Unmarshal(snapshot, &s.data)
		s.mu.Unlock()
		problem(w, err)
		return
	}
	if after != nil {
		after()
	}
	s.mu.Unlock()
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	writeJSON(w, status, value)
}
func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "mode": "local-mvp"})
	})
	s.mux.HandleFunc("GET /v1/capabilities", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"api_version": "v1", "source": "mysql-row-full", "start_modes": []string{"current"}, "transform": "lua-5.1", "sink": "local-jsonl-manifest", "graph": "one source -> one Lua -> 1..16 outputs", "incremental_only": true, "iceberg": false, "s3_buffer": false, "multi_tenant": false})
	})
	s.mux.HandleFunc("GET /v1/connections/mysql-local/inspect", func(w http.ResponseWriter, r *http.Request) {
		tables := strings.Split(r.URL.Query().Get("tables"), ",")
		if len(tables) > 16 || r.URL.Query().Get("tables") == "" {
			problem(w, fail(400, "TABLES_REQUIRED", "provide ?tables=shop.orders (up to 16 comma-separated names)"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		result, err := mysqlcdc.Probe(ctx, s.cfg.MySQL, tables)
		if err != nil {
			problem(w, err)
			return
		}
		writeJSON(w, 200, result)
	})
	s.mux.HandleFunc("POST /v1/functions", func(w http.ResponseWriter, r *http.Request) {
		s.mutate(w, r, func(raw []byte) (any, int, string, func(), error) {
			var req struct {
				Name   string `json:"name"`
				Source string `json:"source"`
			}
			if err := decode(raw, &req); err != nil {
				return nil, 0, "", nil, err
			}
			if req.Name == "" || req.Source == "" || len(req.Source) > 65536 {
				return nil, 0, "", nil, fail(422, "INVALID_FUNCTION", "name and source required; source <=64 KiB")
			}
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			_, err := (luaruntime.Process{Executable: s.cfg.WorkerExecutable}).Execute(ctx, model.TransformRequest{Source: req.Source, Events: []model.Event{}})
			if err != nil {
				return nil, 0, "", nil, fail(422, "INVALID_FUNCTION", err.Error())
			}
			id := "fn_" + hash(req.Source)
			fn := Function{ID: id, Name: req.Name, Source: req.Source, Digest: "sha256:" + hash(req.Source)}
			if old, ok := s.data.Functions[id]; ok {
				fn = old
			} else {
				s.data.Functions[id] = fn
			}
			return fn, 201, "", nil, nil
		})
	})
	s.mux.HandleFunc("GET /v1/functions/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		v, ok := s.data.Functions[r.PathValue("id")]
		s.mu.Unlock()
		if !ok {
			problem(w, fail(404, "NOT_FOUND", "function not found"))
			return
		}
		writeJSON(w, 200, v)
	})
	s.mux.HandleFunc("POST /v1/pipelines/validate", func(w http.ResponseWriter, r *http.Request) {
		raw, err := bodyBytes(w, r)
		if err != nil {
			problem(w, err)
			return
		}
		d, err := pipeline.DecodeJSON(raw)
		if err != nil {
			problem(w, err)
			return
		}
		plan, err := pipeline.Compile(d)
		if err == nil {
			s.mu.Lock()
			_, ok := s.data.Functions[plan.Lua().FunctionRef]
			s.mu.Unlock()
			if !ok {
				err = fail(422, "FUNCTION_NOT_FOUND", "register the referenced function first")
			}
		}
		if err != nil {
			problem(w, err)
			return
		}
		writeJSON(w, 200, plan)
	})
	s.mux.HandleFunc("POST /v1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		s.mutate(w, r, func(raw []byte) (any, int, string, func(), error) {
			d, err := pipeline.DecodeJSON(raw)
			if err != nil {
				return nil, 0, "", nil, err
			}
			plan, err := pipeline.Compile(d)
			if err != nil {
				return nil, 0, "", nil, err
			}
			if _, ok := s.data.Functions[plan.Lua().FunctionRef]; !ok {
				return nil, 0, "", nil, fail(422, "FUNCTION_NOT_FOUND", "register function first")
			}
			for _, existing := range s.data.Pipelines {
				if existing.Definition.Metadata.Name == d.Metadata.Name {
					return nil, 0, "", nil, fail(409, "NAME_EXISTS", "pipeline name already exists")
				}
			}
			id := "pipe_" + hash(r.Header.Get("Idempotency-Key"), string(raw))[:24]
			draft := Draft{ID: id, Revision: 1, Definition: plan.Definition}
			s.data.Pipelines[id] = draft
			return draft, 201, `"1"`, nil, nil
		})
	})
	s.mux.HandleFunc("GET /v1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		items := sortedDrafts(s.data.Pipelines)
		s.mu.Unlock()
		writeJSON(w, 200, map[string]any{"items": items})
	})
	s.mux.HandleFunc("GET /v1/pipelines/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		draft, ok := s.data.Pipelines[r.PathValue("id")]
		s.mu.Unlock()
		if !ok {
			problem(w, fail(404, "NOT_FOUND", "pipeline not found"))
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"%d"`, draft.Revision))
		writeJSON(w, 200, draft)
	})
	s.mux.HandleFunc("PUT /v1/pipelines/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.mutate(w, r, func(raw []byte) (any, int, string, func(), error) {
			id := r.PathValue("id")
			draft, ok := s.data.Pipelines[id]
			if !ok {
				return nil, 0, "", nil, fail(404, "NOT_FOUND", "pipeline not found")
			}
			if r.Header.Get("If-Match") == "" {
				return nil, 0, "", nil, fail(428, "PRECONDITION_REQUIRED", "If-Match required")
			}
			if r.Header.Get("If-Match") != fmt.Sprintf(`"%d"`, draft.Revision) {
				return nil, 0, "", nil, fail(412, "REVISION_CONFLICT", "draft changed")
			}
			d, err := pipeline.DecodeJSON(raw)
			if err != nil {
				return nil, 0, "", nil, err
			}
			plan, err := pipeline.Compile(d)
			if err != nil {
				return nil, 0, "", nil, err
			}
			if _, ok := s.data.Functions[plan.Lua().FunctionRef]; !ok {
				return nil, 0, "", nil, fail(422, "FUNCTION_NOT_FOUND", "function not found")
			}
			for pid, other := range s.data.Pipelines {
				if pid != id && other.Definition.Metadata.Name == d.Metadata.Name {
					return nil, 0, "", nil, fail(409, "NAME_EXISTS", "pipeline name already exists")
				}
			}
			draft.Revision++
			draft.Definition = plan.Definition
			s.data.Pipelines[id] = draft
			return draft, 200, fmt.Sprintf(`"%d"`, draft.Revision), nil, nil
		})
	})
	s.mux.HandleFunc("POST /v1/pipelines/{id}/versions", func(w http.ResponseWriter, r *http.Request) {
		s.mutate(w, r, func(raw []byte) (any, int, string, func(), error) {
			var req struct {
				Revision uint64 `json:"draft_revision,string"`
			}
			if err := decode(raw, &req); err != nil {
				return nil, 0, "", nil, err
			}
			draft, ok := s.data.Pipelines[r.PathValue("id")]
			if !ok {
				return nil, 0, "", nil, fail(404, "NOT_FOUND", "pipeline not found")
			}
			if req.Revision != draft.Revision {
				return nil, 0, "", nil, fail(409, "REVISION_CONFLICT", "draft revision changed")
			}
			plan, err := pipeline.Compile(draft.Definition)
			if err != nil {
				return nil, 0, "", nil, err
			}
			id := "ver_" + hash(draft.ID, plan.Digest)[:24]
			v := Version{ID: id, PipelineID: draft.ID, DraftRevision: draft.Revision, Plan: plan}
			s.data.Versions[id] = v
			return v, 201, "", nil, nil
		})
	})
	s.mux.HandleFunc("GET /v1/pipelines/{id}/versions/{version}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		v, ok := s.data.Versions[r.PathValue("version")]
		s.mu.Unlock()
		if !ok || v.PipelineID != r.PathValue("id") {
			problem(w, fail(404, "NOT_FOUND", "version not found"))
			return
		}
		writeJSON(w, 200, v)
	})
	s.mux.HandleFunc("POST /v1/pipelines/{id}/instances", func(w http.ResponseWriter, r *http.Request) {
		s.mutate(w, r, func(raw []byte) (any, int, string, func(), error) {
			var req struct {
				VersionID string `json:"version_id"`
			}
			if err := decode(raw, &req); err != nil {
				return nil, 0, "", nil, err
			}
			v, ok := s.data.Versions[req.VersionID]
			if !ok || v.PipelineID != r.PathValue("id") {
				return nil, 0, "", nil, fail(404, "NOT_FOUND", "version not found")
			}
			for _, existing := range s.data.Instances {
				if existing.PipelineID == v.PipelineID {
					return nil, 0, "", nil, fail(409, "INSTANCE_EXISTS", "MVP allows one immutable instance per pipeline")
				}
			}
			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
			defer cancel()
			probe, err := mysqlcdc.Probe(ctx, s.cfg.MySQL, v.Plan.Source().Tables)
			if err != nil {
				return nil, 0, "", nil, err
			}
			id := "inst_" + hash(v.PipelineID, r.Header.Get("Idempotency-Key"))[:24]
			i := Instance{ID: id, PipelineID: v.PipelineID, VersionID: v.ID, State: "STARTING", DesiredState: "RUNNING", InitialPosition: probe.Position, SourceID: probe.ServerUUID, Schemas: probe.Tables, ReplicationServerID: s.cfg.MySQL.ServerID + uint32(len(s.data.Instances)) + 1}
			s.data.Instances[id] = i
			return i, 201, "", func() { s.startLocked(id) }, nil
		})
	})
	s.mux.HandleFunc("GET /v1/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		i, ok := s.data.Instances[r.PathValue("id")]
		s.mu.Unlock()
		if !ok {
			problem(w, fail(404, "NOT_FOUND", "instance not found"))
			return
		}
		writeJSON(w, 200, i)
	})
	for _, action := range []string{"pause", "resume"} {
		s.mux.HandleFunc("POST /v1/instances/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
			s.mutate(w, r, func(raw []byte) (any, int, string, func(), error) {
				var empty struct{}
				if err := decode(raw, &empty); err != nil {
					return nil, 0, "", nil, err
				}
				id := r.PathValue("id")
				i, ok := s.data.Instances[id]
				if !ok {
					return nil, 0, "", nil, fail(404, "NOT_FOUND", "instance not found")
				}
				if action == "pause" {
					i.DesiredState = "PAUSED"
					i.State = "PAUSED"
					if _, running := s.workers[id]; running {
						i.State = "PAUSING"
					}
					s.data.Instances[id] = i
					return i, 202, "", func() {
						if cancel, ok := s.workers[id]; ok {
							cancel()
						}
					}, nil
				}
				if i.State == "PAUSING" {
					return nil, 0, "", nil, fail(409, "TRANSITION_IN_PROGRESS", "wait until PAUSED")
				}
				i.DesiredState = "RUNNING"
				i.LastError = ""
				s.data.Instances[id] = i
				return i, 202, "", func() { s.startLocked(id) }, nil
			})
		})
	}
	s.mux.HandleFunc("GET /v1/instances/{id}/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		s.mu.Lock()
		i, ok := s.data.Instances[id]
		s.mu.Unlock()
		if !ok {
			problem(w, fail(404, "NOT_FOUND", "instance not found"))
			return
		}
		cp, exists, err := s.store.Checkpoint(id)
		if err != nil {
			problem(w, err)
			return
		}
		if !exists {
			cp.Position = i.InitialPosition
			cp.VersionID = i.VersionID
		}
		writeJSON(w, 200, cp)
	})
	s.mux.HandleFunc("GET /v1/instances/{id}/outputs/{output}", func(w http.ResponseWriter, r *http.Request) {
		id, output := r.PathValue("id"), r.PathValue("output")
		s.mu.Lock()
		i, ok := s.data.Instances[id]
		v := s.data.Versions[i.VersionID]
		s.mu.Unlock()
		if !ok {
			problem(w, fail(404, "NOT_FOUND", "instance not found"))
			return
		}
		found := false
		for _, out := range v.Plan.Outputs() {
			if out.Name == output {
				found = true
			}
		}
		if !found {
			problem(w, fail(404, "NOT_FOUND", "output not found"))
			return
		}
		snapshot, err := s.store.Snapshot(id, output)
		if err != nil {
			problem(w, err)
			return
		}
		if snapshot.Checkpoint.Sequence == 0 {
			snapshot.Checkpoint.Position = i.InitialPosition
			snapshot.Checkpoint.VersionID = i.VersionID
		}
		writeJSON(w, 200, snapshot)
	})
}
