// Package model contains the shared, lossless contracts of the local MVP.
package model

import "context"

type Position struct {
	File   string `json:"file"`
	Offset uint32 `json:"position"`
}
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}
type Event struct {
	ID             string         `json:"event_id"`
	SourceID       string         `json:"source_id"`
	TransactionID  string         `json:"transaction_id"`
	RowIndex       uint64         `json:"row_index"`
	Database       string         `json:"database"`
	Table          string         `json:"table"`
	Operation      string         `json:"op"`
	Before         map[string]any `json:"before"`
	After          map[string]any `json:"after"`
	Columns        []Column       `json:"columns"`
	SourcePosition Position       `json:"source_position"`
}
type Transaction struct {
	ID          string   `json:"transaction_id"`
	ResumeAfter Position `json:"resume_after"`
	Events      []Event  `json:"events"`
}
type Mutation struct {
	Output    string         `json:"output"`
	Operation string         `json:"op"`
	Key       map[string]any `json:"key"`
	Value     map[string]any `json:"value,omitempty"`
}
type Output struct {
	Name      string   `json:"name"`
	KeyFields []string `json:"key_fields"`
	Fields    []Column `json:"fields"`
}
type Envelope struct {
	MutationID string   `json:"mutation_id"`
	EventID    string   `json:"source_event_id"`
	Index      int      `json:"output_index"`
	Mutation   Mutation `json:"mutation"`
}
type TransformRequest struct {
	Source string  `json:"source"`
	Events []Event `json:"events"`
}
type TransformResponse struct {
	Results [][]Mutation `json:"results"`
	Error   string       `json:"error,omitempty"`
}
type Transformer interface {
	Execute(context.Context, TransformRequest) (TransformResponse, error)
}
