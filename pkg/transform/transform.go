package transform

import (
	"context"
	"encoding/json"

	"example.com/etl/pkg/cdc"
)

type Request struct {
	ProtocolVersion  string      `json:"protocol_version"`
	BatchID          string      `json:"batch_id"`
	TransformVersion string      `json:"transform_version"`
	Events           []cdc.Event `json:"events"`
}
type Record struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
}
type Result struct {
	EventID string   `json:"event_id"`
	Records []Record `json:"records"`
}
type Response struct {
	ProtocolVersion string   `json:"protocol_version"`
	BatchID         string   `json:"batch_id"`
	Results         []Result `json:"results"`
}

// Executor must enforce isolated instances, immutable code identity, limits,
// lossless JSON types and one validated result per input event. No backend is
// implemented in this skeleton.
type Executor interface {
	Execute(context.Context, Request) (Response, error)
	Close(context.Context) error
}
