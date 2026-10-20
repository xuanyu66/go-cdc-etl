// Package pipeline defines and compiles the supported local MVP graph.
package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"

	"example.com/etl/internal/localstore"
	"example.com/etl/internal/model"
)

const (
	SourceMySQL  = "source.mysql"
	TransformLua = "transform.lua"
	SinkLocal    = "sink.local"
)

type Definition struct {
	APIVersion string   `json:"api_version"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       Spec     `json:"spec"`
}
type Metadata struct {
	Name string `json:"name"`
}
type Spec struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}
type Node struct {
	ID    string       `json:"id"`
	Type  string       `json:"type"`
	MySQL *MySQLConfig `json:"mysql,omitempty"`
	Lua   *LuaConfig   `json:"lua,omitempty"`
	Local *LocalConfig `json:"local,omitempty"`
}
type Start struct {
	Mode string `json:"mode"`
}
type MySQLConfig struct {
	ConnectionRef string   `json:"connection_ref"`
	Tables        []string `json:"tables"`
	Start         Start    `json:"start"`
}
type LuaConfig struct {
	FunctionRef string `json:"function_ref"`
	TimeoutMS   int    `json:"timeout_ms"`
}
type LocalConfig struct {
	Output    string         `json:"output"`
	KeyFields []string       `json:"key_fields"`
	Fields    []model.Column `json:"fields"`
}
type Violation struct {
	Pointer string `json:"pointer"`
	Message string `json:"message"`
}
type ValidationError struct {
	Violations []Violation `json:"errors"`
}

func (e *ValidationError) Error() string { return fmt.Sprintf("invalid pipeline: %v", e.Violations) }

type Plan struct {
	Digest        string     `json:"digest"`
	SourceNode    string     `json:"source_node"`
	TransformNode string     `json:"transform_node"`
	RequiredSinks []string   `json:"required_sinks"`
	Definition    Definition `json:"definition"`
}

var (
	namePattern  = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	tablePattern = regexp.MustCompile(`^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$`)
	typePattern  = regexp.MustCompile(`^(string|bool|int64|uint64|float64|decimal\([1-9][0-9]?,[0-9]{1,2}\))$`)
)

// Compile intentionally supports one source -> one Lua transform -> N local
// sinks. Arbitrary multi-stage graphs, joins and non-local sinks are rejected.
func Compile(d Definition) (Plan, error) {
	e := &ValidationError{}
	add := func(p, m string) { e.Violations = append(e.Violations, Violation{p, m}) }
	if d.APIVersion != "v1" {
		add("/api_version", "must be v1")
	}
	if d.Kind != "Pipeline" {
		add("/kind", "must be Pipeline")
	}
	if !namePattern.MatchString(d.Metadata.Name) {
		add("/metadata/name", "invalid identifier")
	}
	if len(d.Spec.Nodes) < 3 || len(d.Spec.Nodes) > 18 {
		add("/spec/nodes", "requires source, Lua and 1..16 sinks")
	}
	nodes := map[string]Node{}
	source, transform := "", ""
	var sinks []string
	outputs := map[string]bool{}
	for i, n := range d.Spec.Nodes {
		p := fmt.Sprintf("/spec/nodes/%d", i)
		if !namePattern.MatchString(n.ID) {
			add(p+"/id", "invalid identifier")
		}
		if _, ok := nodes[n.ID]; ok {
			add(p+"/id", "duplicate node")
		}
		nodes[n.ID] = n
		count := 0
		if n.MySQL != nil {
			count++
		}
		if n.Lua != nil {
			count++
		}
		if n.Local != nil {
			count++
		}
		if count != 1 {
			add(p, "exactly one matching configuration is required")
		}
		switch n.Type {
		case SourceMySQL:
			if source != "" {
				add(p, "only one source allowed")
			}
			source = n.ID
			if n.MySQL == nil {
				add(p, "mysql config required")
				continue
			}
			c := n.MySQL
			if c.ConnectionRef != "mysql-local" {
				add(p+"/mysql/connection_ref", "MVP supports mysql-local only")
			}
			if c.Start.Mode != "current" {
				add(p+"/mysql/start/mode", "MVP supports current only; restarts use saved checkpoint")
			}
			if len(c.Tables) < 1 || len(c.Tables) > 16 {
				add(p+"/mysql/tables", "requires 1..16 fixed tables")
			}
			seen := map[string]bool{}
			for _, t := range c.Tables {
				if !tablePattern.MatchString(t) || seen[t] {
					add(p+"/mysql/tables", "requires unique database.table names")
				}
				seen[t] = true
			}
		case TransformLua:
			if transform != "" {
				add(p, "only one transform allowed")
			}
			transform = n.ID
			if n.Lua == nil {
				add(p, "lua config required")
				continue
			}
			if n.Lua.FunctionRef == "" {
				add(p+"/lua/function_ref", "required")
			}
			if n.Lua.TimeoutMS < 1 || n.Lua.TimeoutMS > 5000 {
				add(p+"/lua/timeout_ms", "must be 1..5000")
			}
		case SinkLocal:
			sinks = append(sinks, n.ID)
			if n.Local == nil {
				add(p, "local config required")
				continue
			}
			c := n.Local
			if !namePattern.MatchString(c.Output) || outputs[c.Output] {
				add(p+"/local/output", "output names must be valid and unique")
			}
			outputs[c.Output] = true
			if len(c.Fields) < 1 || len(c.Fields) > 100 {
				add(p+"/local/fields", "requires 1..100 fields")
			}
			fields := map[string]model.Column{}
			for _, f := range c.Fields {
				if !namePattern.MatchString(f.Name) || !typePattern.MatchString(f.Type) {
					add(p+"/local/fields", "invalid field name or unsupported type")
				}
				if _, ok := fields[f.Name]; ok {
					add(p+"/local/fields", "duplicate field")
				}
				fields[f.Name] = f
			}
			if len(c.KeyFields) == 0 {
				add(p+"/local/key_fields", "at least one key required")
			}
			keys := map[string]bool{}
			for _, key := range c.KeyFields {
				f, ok := fields[key]
				if !ok || !f.Required || keys[key] || f.Type == "float64" {
					add(p+"/local/key_fields", "keys must be unique required non-float fields")
				}
				keys[key] = true
			}
		default:
			add(p+"/type", "MVP supports source.mysql, transform.lua and sink.local only")
		}
	}
	if source == "" || transform == "" || len(sinks) == 0 {
		add("/spec/nodes", "requires one MySQL source, one Lua transform and at least one sink")
	}
	expected := map[Edge]bool{{source, transform}: true}
	for _, id := range sinks {
		expected[Edge{transform, id}] = true
	}
	seenEdges := map[Edge]bool{}
	for i, edge := range d.Spec.Edges {
		if !expected[edge] || seenEdges[edge] {
			add(fmt.Sprintf("/spec/edges/%d", i), "unsupported or duplicate edge; requires source -> Lua -> sinks")
		}
		seenEdges[edge] = true
	}
	if len(seenEdges) != len(expected) {
		add("/spec/edges", "missing required edge")
	}
	if len(sinks) > 0 {
		if err := localstore.Validate((Plan{Definition: d}).Outputs(), nil); err != nil {
			add("/spec/nodes", err.Error())
		}
	}
	if len(e.Violations) > 0 {
		return Plan{}, e
	}
	sort.Strings(sinks)
	raw, _ := json.Marshal(d)
	var canonical Definition
	_ = json.Unmarshal(raw, &canonical)
	sort.Slice(canonical.Spec.Nodes, func(i, j int) bool { return canonical.Spec.Nodes[i].ID < canonical.Spec.Nodes[j].ID })
	for _, n := range canonical.Spec.Nodes {
		if n.MySQL != nil {
			sort.Strings(n.MySQL.Tables)
		}
	}
	sort.Slice(canonical.Spec.Edges, func(i, j int) bool {
		a, b := canonical.Spec.Edges[i], canonical.Spec.Edges[j]
		if a.From == b.From {
			return a.To < b.To
		}
		return a.From < b.From
	})
	raw, _ = json.Marshal(canonical)
	hash := sha256.Sum256(raw)
	return Plan{Digest: "sha256:" + hex.EncodeToString(hash[:]), SourceNode: source, TransformNode: transform, RequiredSinks: sinks, Definition: canonical}, nil
}

func (p Plan) Source() MySQLConfig {
	for _, n := range p.Definition.Spec.Nodes {
		if n.MySQL != nil {
			return *n.MySQL
		}
	}
	return MySQLConfig{}
}

func (p Plan) Lua() LuaConfig {
	for _, n := range p.Definition.Spec.Nodes {
		if n.Lua != nil {
			return *n.Lua
		}
	}
	return LuaConfig{}
}

func (p Plan) Outputs() []model.Output {
	var out []model.Output
	for _, n := range p.Definition.Spec.Nodes {
		if n.Local != nil {
			out = append(out, model.Output{Name: n.Local.Output, KeyFields: n.Local.KeyFields, Fields: n.Local.Fields})
		}
	}
	return out
}
