package checkpoint

import (
	"context"
	"reflect"
	"regexp"
	"sync"
	"time"
)

// MemoryStore is an executable specification for tests and examples ONLY. It
// loses all progress on process restart and must not back a real CDC pipeline.
type MemoryStore struct {
	mu     sync.Mutex
	now    func() time.Time
	states map[Scope]*memoryState
}
type memoryState struct {
	lease      Lease
	lastFence  uint64
	cursor     Cursor
	prepared   *Prepared
	receipts   map[Write]Receipt
	lastCommit *CommitRecord
}

func NewMemoryStore(now func() time.Time) *MemoryStore {
	if now == nil {
		now = time.Now
	}
	return &MemoryStore{now: now, states: make(map[Scope]*memoryState)}
}

func (m *MemoryStore) Acquire(ctx context.Context, scope Scope, owner string, ttl time.Duration) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if scope.TenantID == "" || scope.InstanceID == "" || scope.Generation == "" || owner == "" || ttl <= 0 {
		return Lease{}, ErrInvalid
	}
	s := m.states[scope]
	if s == nil {
		s = &memoryState{}
		m.states[scope] = s
	}
	if m.now().Before(s.lease.ExpiresAt) {
		return Lease{}, ErrConflict
	}
	s.lastFence++
	s.lease = Lease{Scope: scope, Owner: owner, Fence: s.lastFence, ExpiresAt: m.now().Add(ttl)}
	return s.lease, nil
}

func (m *MemoryStore) owned(l Lease) (*memoryState, error) {
	s := m.states[l.Scope]
	if s == nil || l.Fence != s.lease.Fence || l.Owner != s.lease.Owner || !m.now().Before(s.lease.ExpiresAt) {
		return nil, ErrFenced
	}
	return s, nil
}

func (m *MemoryStore) Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if ttl <= 0 {
		return Lease{}, ErrInvalid
	}
	s, err := m.owned(l)
	if err != nil {
		return Lease{}, err
	}
	s.lease.ExpiresAt = m.now().Add(ttl)
	return s.lease, nil
}

func (m *MemoryStore) Load(ctx context.Context, scope Scope) (Cursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Cursor{}, err
	}
	if s := m.states[scope]; s != nil {
		return s.cursor, nil
	}
	return Cursor{}, nil
}

func clonePrepared(p Prepared) Prepared { p.Required = append([]Write(nil), p.Required...); return p }

func cloneReceipt(r Receipt) Receipt { r.Objects = append([]Object(nil), r.Objects...); return r }

func cloneRecord(r CommitRecord) CommitRecord {
	r.Receipts = append([]Receipt(nil), r.Receipts...)
	for i := range r.Receipts {
		r.Receipts[i] = cloneReceipt(r.Receipts[i])
	}
	return r
}

func (m *MemoryStore) Prepare(ctx context.Context, l Lease, expectedRevision uint64, p Prepared) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := m.owned(l)
	if err != nil {
		return err
	}
	if s.cursor.Revision != expectedRevision || p.Sequence != s.cursor.Sequence+1 {
		return ErrConflict
	}
	if p.TransactionID == "" || p.PlanDigest == "" || len(p.Required) == 0 {
		return ErrInvalid
	}
	if p.ResumeAfter.GTIDSet == "" && (p.ResumeAfter.File == "" || p.ResumeAfter.Offset < 4) {
		return ErrInvalid
	}
	seen := map[Write]bool{}
	for _, w := range p.Required {
		if w.SinkID == "" || w.BatchID == "" || seen[w] {
			return ErrInvalid
		}
		seen[w] = true
	}
	if s.prepared != nil {
		if reflect.DeepEqual(*s.prepared, p) {
			return nil
		}
		return ErrConflict
	}
	copy := clonePrepared(p)
	s.prepared = &copy
	s.receipts = make(map[Write]Receipt)
	return nil
}

var checksum = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (m *MemoryStore) RecordReceipt(ctx context.Context, l Lease, sequence uint64, receipt Receipt) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := m.owned(l)
	if err != nil {
		return err
	}
	if s.prepared == nil || s.prepared.Sequence != sequence {
		return ErrConflict
	}
	required := false
	for _, w := range s.prepared.Required {
		if w == receipt.Write {
			required = true
			break
		}
	}
	if !required || receipt.Empty != (len(receipt.Objects) == 0) {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, object := range receipt.Objects {
		if object.Key == "" || object.Bytes <= 0 || !checksum.MatchString(object.SHA256) || seen[object.Key] {
			return ErrInvalid
		}
		seen[object.Key] = true
	}
	if previous, ok := s.receipts[receipt.Write]; ok {
		if reflect.DeepEqual(previous, receipt) {
			return nil
		}
		return ErrConflict
	}
	s.receipts[receipt.Write] = cloneReceipt(receipt)
	return nil
}

func (m *MemoryStore) Commit(ctx context.Context, l Lease, expectedRevision, sequence uint64) (CommitRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return CommitRecord{}, err
	}
	s, err := m.owned(l)
	if err != nil {
		return CommitRecord{}, err
	}
	// A lost success response can be retried without incrementing the cursor.
	if s.lastCommit != nil && s.cursor.Sequence == sequence && s.cursor.Revision == expectedRevision+1 {
		return cloneRecord(*s.lastCommit), nil
	}
	if s.cursor.Revision != expectedRevision || sequence != s.cursor.Sequence+1 || s.prepared == nil || s.prepared.Sequence != sequence {
		return CommitRecord{}, ErrConflict
	}
	var receipts []Receipt
	for _, w := range s.prepared.Required {
		r, ok := s.receipts[w]
		if !ok {
			return CommitRecord{}, ErrIncomplete
		}
		receipts = append(receipts, cloneReceipt(r))
	}
	cursor := Cursor{Revision: expectedRevision + 1, Sequence: sequence, TransactionID: s.prepared.TransactionID, ResumeAfter: s.prepared.ResumeAfter}
	record := CommitRecord{Cursor: cursor, PlanDigest: s.prepared.PlanDigest, Receipts: receipts}
	s.cursor = cursor
	s.lastCommit = &record
	s.prepared = nil
	s.receipts = nil
	return cloneRecord(record), nil
}

var _ Store = (*MemoryStore)(nil)
