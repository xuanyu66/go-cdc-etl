package sink

import (
	"context"
	"encoding/json"

	"example.com/etl/pkg/checkpoint"
)

type Request struct {
	Scope         checkpoint.Scope
	Fence         uint64
	Sequence      uint64
	TransactionID string
	PlanDigest    string
	Write         checkpoint.Write
	AttemptID     string
	// Trusted envelopes are built by the execution engine, not user code.
	Records []json.RawMessage
}

type Writer interface {
	// Persist confirms complete object durability (or explicit empty output).
	// It must not submit the source checkpoint or report global DAG success.
	// A timeout may have persisted objects: retry is at least once.
	Persist(context.Context, Request) (checkpoint.Receipt, error)
}
