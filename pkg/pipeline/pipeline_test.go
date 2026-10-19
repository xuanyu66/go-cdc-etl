package pipeline

import (
	"encoding/json"
	"os"
	"testing"
)

func fixture(t *testing.T) Definition {
	t.Helper()
	raw, err := os.ReadFile("../../examples/pipeline.json")
	if err != nil {
		t.Fatal(err)
	}
	d, err := DecodeJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCompileMVP(t *testing.T) {
	d := fixture(t)
	a, err := Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	if a.SourceNode != "mysql" || a.TransformNode != "normalize" || len(a.RequiredSinks) != 1 {
		t.Fatalf("bad plan: %+v", a)
	}
	d.Spec.Nodes[0], d.Spec.Nodes[2] = d.Spec.Nodes[2], d.Spec.Nodes[0]
	d.Spec.Edges[0], d.Spec.Edges[1] = d.Spec.Edges[1], d.Spec.Edges[0]
	b, err := Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	if a.Digest != b.Digest {
		t.Fatal("node/edge order changed plan identity")
	}
}

func TestRejectUnsupportedCapabilities(t *testing.T) {
	cases := map[string]func(*Definition){
		"wildcard":         func(d *Definition) { d.Spec.Nodes[0].MySQL.Tables = []string{"shop.*"} },
		"historical-start": func(d *Definition) { d.Spec.Nodes[0].MySQL.Start.Mode = "file_position" },
		"wrong-backend":    func(d *Definition) { d.Spec.Nodes[1].Type = "transform.wasm" },
		"duplicate-edge":   func(d *Definition) { d.Spec.Edges = append(d.Spec.Edges, d.Spec.Edges[0]) },
		"cycle":            func(d *Definition) { d.Spec.Edges = append(d.Spec.Edges, Edge{"archive", "mysql"}) },
		"missing-edge":     func(d *Definition) { d.Spec.Edges = d.Spec.Edges[:1] },
		"decimal-scale":    func(d *Definition) { d.Spec.Nodes[2].Local.Fields[2].Type = "decimal(2,3)" },
		"nullable-key":     func(d *Definition) { d.Spec.Nodes[2].Local.Fields[0].Required = false },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			d := fixture(t)
			mutate(&d)
			if _, err := Compile(d); err == nil {
				t.Fatal("unsupported configuration accepted")
			}
		})
	}
}

func TestStrictDecode(t *testing.T) {
	d := fixture(t)
	raw, _ := json.Marshal(d)
	for _, bad := range []string{`{"api_version":"v1","api_version":"v1"}`, `{"unknown":1}`, string(raw) + "{}", `null []`} {
		if _, err := DecodeJSON([]byte(bad)); err == nil {
			t.Fatalf("accepted malformed definition: %s", bad)
		}
	}
}
