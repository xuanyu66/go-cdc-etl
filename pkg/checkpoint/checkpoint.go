// Package checkpoint separates durable sink receipts from globally committed
// progress. Store implementations MUST perform Commit checks and updates in one
// metadata transaction. S3 upload and metadata commit are not atomic together.
package checkpoint

import (
	"context"
	"errors"
	"time"

	"example.com/etl/pkg/cdc"
)

var (
	ErrFenced     = errors.New("lease missing, expired or fenced")
	ErrConflict   = errors.New("checkpoint revision or prepared plan conflict")
	ErrIncomplete = errors.New("required sink writes are incomplete")
	ErrInvalid    = errors.New("invalid checkpoint request")
)

// Scope is derived from authenticated ownership, not trusted request JSON.
type Scope struct{ TenantID, InstanceID, Generation string }
type Lease struct {
	Scope     Scope
	Owner     string
	Fence     uint64
	ExpiresAt time.Time
}
type Cursor struct {
	Revision      uint64       `json:"revision,string"`
	Sequence      uint64       `json:"sequence,string"`
	TransactionID string       `json:"transaction_id"`
	ResumeAfter   cdc.Position `json:"resume_after"`
}
type Write struct {
	SinkID  string
	BatchID string
}
type Prepared struct {
	Sequence      uint64
	TransactionID string
	PlanDigest    string
	ResumeAfter   cdc.Position
	// Every required sink/batch, including empty output, must have a receipt.
	Required []Write
}
type Object struct {
	Key    string
	SHA256 string
	Bytes  int64
}
type Receipt struct {
	Write   Write
	Objects []Object
	// Empty must be explicit; an accidentally missing object is not an ack.
	Empty bool
}
type CommitRecord struct {
	Cursor     Cursor
	PlanDigest string
	Receipts   []Receipt
}

type Store interface {
	// Acquire rejects a live lease, including one with the same owner. On
	// takeover the fencing token increases. TTL is measured by store time.
	Acquire(context.Context, Scope, string, time.Duration) (Lease, error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Load(context.Context, Scope) (Cursor, error)
	// Prepare is immutable and idempotent only for exactly the same plan.
	Prepare(context.Context, Lease, uint64, Prepared) error
	// RecordReceipt acknowledges durability, never advances the checkpoint.
	RecordReceipt(context.Context, Lease, uint64, Receipt) error
	// Commit validates live ownership, expected revision, contiguous sequence,
	// and ALL required receipts. Writes commit record and cursor atomically.
	Commit(context.Context, Lease, uint64, uint64) (CommitRecord, error)
}
