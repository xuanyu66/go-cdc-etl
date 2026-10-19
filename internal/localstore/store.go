// Package localstore implements a local JSONL sink. It is not an Iceberg table.
// A durably renamed manifest is the only commit point for every output and its checkpoint.
package localstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"syscall"

	"example.com/etl/internal/model"
)

const maxFileSize = 64 << 20

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)

type CommitRequest struct {
	InstanceID    string           `json:"instance_id"`
	Sequence      uint64           `json:"sequence"`
	TransactionID string           `json:"transaction_id"`
	VersionID     string           `json:"version_id"`
	ResumeAfter   model.Position   `json:"resume_after"`
	Outputs       []model.Output   `json:"outputs"`
	Records       []model.Envelope `json:"records"`
}
type Checkpoint struct {
	Sequence      uint64         `json:"sequence"`
	TransactionID string         `json:"transaction_id"`
	VersionID     string         `json:"version_id"`
	Position      model.Position `json:"position"`
}
type fileRef struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}
type commit struct {
	Request CommitRequest      `json:"request"`
	Digest  string             `json:"digest"`
	Files   map[string]fileRef `json:"files"`
}
type manifest struct {
	Commits []commit `json:"commits"`
}
type Store struct {
	root string
	mu   sync.Mutex
	lock *os.File
	// Tests simulate a process stopping after durable data and before publication.
	beforePublish func() error
}

func Open(root string) (*Store, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(abs, 0700); err != nil {
		return nil, err
	}
	abs, err = filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(abs, ".localstore.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("store already locked: %w", err)
	}
	return &Store{root: abs, lock: f}, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return nil
	}
	syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	e := s.lock.Close()
	s.lock = nil
	return e
}

func (s *Store) dir(id string) (string, error) {
	if !identifier.MatchString(id) {
		return "", errors.New("invalid instance identifier")
	}
	dir := filepath.Join(s.root, id)
	if st, e := os.Lstat(dir); e == nil && st.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("instance directory cannot be symlink")
	}
	return dir, nil
}
func decode(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if e := d.Decode(v); e != nil {
		return e
	}
	if e := d.Decode(new(any)); e != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}
func readLimited(path string) ([]byte, error) {
	st, e := os.Lstat(path)
	if e != nil {
		return nil, e
	}
	if !st.Mode().IsRegular() || st.Size() > maxFileSize {
		return nil, errors.New("invalid or oversized store file")
	}
	return os.ReadFile(path)
}
func (s *Store) load(id string) (manifest, string, error) {
	dir, e := s.dir(id)
	if e != nil {
		return manifest{}, "", e
	}
	b, e := readLimited(filepath.Join(dir, "manifest.json"))
	if os.IsNotExist(e) {
		return manifest{}, dir, nil
	}
	if e != nil {
		return manifest{}, dir, e
	}
	var m manifest
	e = decode(b, &m)
	return m, dir, e
}
func cp(c commit) Checkpoint {
	return Checkpoint{Sequence: c.Request.Sequence, TransactionID: c.Request.TransactionID, VersionID: c.Request.VersionID, Position: c.Request.ResumeAfter}
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func syncDir(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func atomicFile(dir, name string, data []byte) error {
	if len(data) > maxFileSize {
		return errors.New("store file exceeds 64 MiB limit")
	}
	f, e := os.CreateTemp(dir, ".pending-")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, e = f.Write(data); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(tmp, filepath.Join(dir, name)); e != nil {
		return e
	}
	return syncDir(dir)
}

func (s *Store) Commit(ctx context.Context, r CommitRequest) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return Checkpoint{}, errors.New("store closed")
	}
	if e := ctx.Err(); e != nil {
		return Checkpoint{}, e
	}
	if r.TransactionID == "" || r.VersionID == "" {
		return Checkpoint{}, errors.New("transaction and version required")
	}
	if e := Validate(r.Outputs, r.Records); e != nil {
		return Checkpoint{}, e
	}
	raw, e := json.Marshal(r)
	if e != nil {
		return Checkpoint{}, e
	}
	if len(raw) > maxFileSize {
		return Checkpoint{}, errors.New("transaction exceeds 64 MiB limit")
	}
	hash := digest(raw)
	m, dir, e := s.load(r.InstanceID)
	if e != nil {
		return Checkpoint{}, e
	}
	for _, c := range m.Commits {
		if c.Request.TransactionID == r.TransactionID {
			if c.Digest != hash {
				return Checkpoint{}, errors.New("CONTENT_CONFLICT: transaction payload changed")
			}
			return cp(c), nil
		}
	}
	if r.Sequence != uint64(len(m.Commits))+1 {
		return Checkpoint{}, errors.New("SEQUENCE_GAP: transaction sequence must be contiguous")
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return Checkpoint{}, e
	}
	if e = syncDir(s.root); e != nil {
		return Checkpoint{}, e
	}
	c := commit{Request: r, Digest: hash, Files: map[string]fileRef{}}
	c.Request.Records = nil
	// Empty output files are explicit completion markers for filtered transactions.
	for _, o := range r.Outputs {
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		for _, record := range r.Records {
			if record.Mutation.Output == o.Name {
				if e = enc.Encode(record); e != nil {
					return Checkpoint{}, e
				}
			}
		}
		name := fmt.Sprintf("%020d-%s-%s.jsonl", r.Sequence, hash, o.Name)
		data := b.Bytes()
		if existing, err := readLimited(filepath.Join(dir, name)); err == nil {
			if !bytes.Equal(existing, data) {
				return Checkpoint{}, errors.New("CONTENT_CONFLICT: existing data file differs")
			}
		} else if !os.IsNotExist(err) {
			return Checkpoint{}, err
		} else if e = atomicFile(dir, name, data); e != nil {
			return Checkpoint{}, e
		}
		c.Files[o.Name] = fileRef{Name: name, SHA256: digest(data)}
	}
	if e = ctx.Err(); e != nil {
		return Checkpoint{}, e
	}
	if s.beforePublish != nil {
		if e = s.beforePublish(); e != nil {
			return Checkpoint{}, e
		}
	}
	m.Commits = append(m.Commits, c)
	data, e := json.Marshal(m)
	if e != nil {
		return Checkpoint{}, e
	}
	if e = atomicFile(dir, "manifest.json", data); e != nil {
		return Checkpoint{}, e
	}
	return cp(c), nil
}
func (s *Store) Checkpoint(id string) (Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, _, e := s.load(id)
	if e != nil {
		return Checkpoint{}, false, e
	}
	if len(m.Commits) == 0 {
		return Checkpoint{}, false, nil
	}
	return cp(m.Commits[len(m.Commits)-1]), true, nil
}
func (s *Store) commits(id string) ([]CommitRequest, error) {
	m, dir, e := s.load(id)
	if e != nil {
		return nil, e
	}
	out := make([]CommitRequest, 0, len(m.Commits))
	for _, c := range m.Commits {
		r := c.Request
		r.Records = nil
		for _, o := range r.Outputs {
			ref, ok := c.Files[o.Name]
			if !ok || filepath.Base(ref.Name) != ref.Name {
				return nil, errors.New("invalid manifest file reference")
			}
			b, e := readLimited(filepath.Join(dir, ref.Name))
			if e != nil {
				return nil, e
			}
			if digest(b) != ref.SHA256 {
				return nil, errors.New("data checksum mismatch")
			}
			d := json.NewDecoder(bytes.NewReader(b))
			d.UseNumber()
			for {
				var v model.Envelope
				if e = d.Decode(&v); e == io.EOF {
					break
				}
				if e != nil {
					return nil, e
				}
				r.Records = append(r.Records, v)
			}
		}
		out = append(out, r)
	}
	return out, nil
}
func (s *Store) Commits(id string) ([]CommitRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits(id)
}
func (s *Store) Rows(id, output string) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	commits, e := s.commits(id)
	if e != nil {
		return nil, e
	}
	rows := map[string]map[string]any{}
	for _, c := range commits {
		var definition model.Output
		for _, o := range c.Outputs {
			if o.Name == output {
				definition = o
				break
			}
		}
		for _, r := range c.Records {
			m := r.Mutation
			if m.Output != output {
				continue
			}
			// Equivalent exact numeric key spellings identify the same row.
			canonical := make(map[string]any, len(m.Key))
			for k, v := range m.Key {
				canonical[k] = v
			}
			for _, f := range definition.Fields {
				if v, ok := canonical[f.Name]; ok {
					if f.Type == "int64" || f.Type == "uint64" || decimalType.MatchString(f.Type) {
						if n, ok := new(big.Rat).SetString(v.(string)); ok {
							canonical[f.Name] = n.RatString()
						}
					}
				}
			}
			b, e := json.Marshal(canonical)
			if e != nil {
				return nil, e
			}
			key := string(b)
			if m.Operation == "delete" {
				delete(rows, key)
			} else {
				rows[key] = m.Value
			}
		}
	}
	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, rows[k])
	}
	return out, nil
}
