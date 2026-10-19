// Package cdc defines runtime-neutral CDC records. Values retain the lossless
// representations specified in docs/design.md; adapters must not coerce strings
// holding integer or decimal values into float64.
package cdc

import "encoding/json"

// Position is a recovery cursor AFTER a complete committed transaction.
// GTIDSet describes the consumed set, not just the last transaction's GTID.
type Position struct {
	GTIDSet string `json:"gtid_set,omitempty"`
	File    string `json:"file,omitempty"`
	Offset  uint64 `json:"position,string,omitempty"`
}

type Event struct {
	ID             string          `json:"event_id"`
	SourceID       string          `json:"source_id"`
	TransactionID  string          `json:"transaction_id"`
	RowIndex       uint64          `json:"row_index"`
	Database       string          `json:"database"`
	Table          string          `json:"table"`
	SchemaVersion  string          `json:"schema_version"`
	Operation      string          `json:"op"`
	SourcePosition json.RawMessage `json:"source_position"`
	Columns        json.RawMessage `json:"columns"`
	Before         json.RawMessage `json:"before"`
	After          json.RawMessage `json:"after"`
}

// Transaction is yielded only after COMMIT. Sequence is assigned and persisted
// by the instance planner; it is not a MySQL GTID numeric component.
type Transaction struct {
	ID          string
	Sequence    uint64
	ResumeAfter Position
	Events      []Event
}
