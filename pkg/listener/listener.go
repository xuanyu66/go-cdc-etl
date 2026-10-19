// Package listener defines one active ordered stream per source assignment.
package listener

import (
	"context"
	"time"

	"example.com/etl/pkg/cdc"
)

type Assignment struct {
	TenantID      string
	InstanceID    string
	SourceID      string
	ConnectionRef string
	Tables        []string
	ResumeAfter   cdc.Position
	Fence         uint64
}

type Factory interface {
	// Open validates a transaction-boundary cursor and the matching schema.
	// A stale/unavailable cursor must fail, never fall forward to latest.
	Open(context.Context, Assignment) (Stream, error)
}

type Stream interface {
	// Next returns only complete committed transactions and respects cancellation
	// and bounded buffering. It does not persist or acknowledge checkpoints.
	Next(context.Context) (cdc.Transaction, error)
	Close() error
}

type Metrics struct {
	BinlogBytesPerSecond float64
	OldestPendingAge     time.Duration
	BufferedBytes        int64
	PendingTransactions  int64
	RetentionHeadroom    time.Duration
}

// ScaleRecommendation distinguishes reader placement from downstream capacity.
// An assignment always has one active reader; reader replicas distribute
// different assignments. A hot source cannot be split by arbitrary offsets.
type ScaleRecommendation struct {
	ReaderReplicas   int
	TransformWorkers int
	SinkWorkers      int
	Reason           string
}

type Autoscaler interface {
	Recommend(context.Context, []Metrics) (ScaleRecommendation, error)
}
