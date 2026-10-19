// Package buffer describes an OPTIONAL durable handoff, not yet implemented.
// It archives normalized committed CDC transactions plus their schema history,
// rather than assuming raw binlog bytes can be replayed without metadata.
package buffer

import (
	"context"

	"example.com/etl/pkg/cdc"
	"example.com/etl/pkg/checkpoint"
)

type Segment struct {
	Scope         checkpoint.Scope
	FirstSequence uint64
	LastSequence  uint64
	ResumeAfter   cdc.Position
	SchemaRefs    []string
	Object        checkpoint.Object
}

type Archive interface {
	// Append uploads an immutable bounded segment. It does not advance either
	// the archive watermark or the delivery checkpoint.
	Append(context.Context, checkpoint.Scope, []cdc.Transaction) (Segment, error)
	// Read must verify checksum/schema availability and emit complete ordered
	// transactions from a segment obtained from Catalog, never from S3 LIST.
	Read(context.Context, Segment) ([]cdc.Transaction, error)
}

type Watermark struct {
	Revision        uint64
	ArchivedThrough uint64
	ResumeAfter     cdc.Position
}

type Catalog interface {
	Load(context.Context, checkpoint.Scope) (Watermark, error)
	// Publish atomically fences ownership, validates contiguous sequence and
	// expected revision, inserts the verified segment and advances ONLY the
	// archive watermark. Production implementation requires durable metadata.
	Publish(context.Context, checkpoint.Lease, uint64, Segment) (Watermark, error)
	// Next returns the segment containing afterSequence+1, or an explicit
	// not-yet-available result. Gaps or missing objects are errors, not EOF.
	Next(context.Context, checkpoint.Scope, uint64) (Segment, bool, error)
}
