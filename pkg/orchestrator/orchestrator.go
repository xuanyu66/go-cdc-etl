package orchestrator

import (
	"context"
	"time"

	"example.com/etl/pkg/pipeline"
)

type Version struct {
	ID         string        `json:"version_id"`
	PipelineID string        `json:"pipeline_id"`
	Plan       pipeline.Plan `json:"plan"`
	CreatedAt  time.Time     `json:"created_at"`
}
type Instance struct {
	ID         string `json:"instance_id"`
	PipelineID string `json:"pipeline_id"`
	VersionID  string `json:"version_id"`
	Generation string `json:"generation"`
	State      string `json:"state"`
}
type CreateInstanceRequest struct {
	VersionID         string `json:"version_id"`
	SchemaBaselineRef string `json:"schema_baseline_ref"`
}
type Repository interface {
	// Publish must check expected draft revision and resolve tenant ownership
	// of connection/artifact references before saving an immutable plan.
	Publish(context.Context, string, string, uint64) (Version, error)
	// CreateInstance assigns a new generation. Versions cannot change in place.
	CreateInstance(context.Context, string, string, CreateInstanceRequest) (Instance, error)
}
type Controller interface {
	// Reconcile is idempotent; lease acquisition gates listener ownership.
	Reconcile(context.Context, string, string) error
	// Pause drains the current transaction before PAUSED; failures preserve
	// its old checkpoint. Resume never changes version or source cursor.
	Pause(context.Context, string, string) error
	Resume(context.Context, string, string) error
}
