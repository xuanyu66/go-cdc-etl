package localstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"example.com/etl/internal/model"
)

var (
	decimalType  = regexp.MustCompile(`^decimal\(([1-9][0-9]*),([0-9]+)\)$`)
	decimalValue = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)
)

// Validate checks the output contract and every mutation before any file is written.
// Exact integer/decimal values cross the transform boundary as strings.
func Validate(outputs []model.Output, records []model.Envelope) error {
	if len(outputs) == 0 {
		return errors.New("at least one output required")
	}
	definitions := map[string]model.Output{}
	for _, o := range outputs {
		if !identifier.MatchString(o.Name) {
			return fmt.Errorf("invalid output name %q", o.Name)
		}
		if _, ok := definitions[o.Name]; ok {
			return errors.New("duplicate output name")
		}
		definitions[o.Name] = o
		fields := map[string]model.Column{}
		for _, f := range o.Fields {
			if f.Name == "" {
				return errors.New("empty field name")
			}
			if _, ok := fields[f.Name]; ok {
				return errors.New("duplicate schema field")
			}
			if e := validType(f.Type); e != nil {
				return e
			}
			fields[f.Name] = f
		}
		if len(o.KeyFields) == 0 {
			return errors.New("key_fields required")
		}
		keys := map[string]bool{}
		for _, key := range o.KeyFields {
			f, ok := fields[key]
			if !ok || !f.Required || keys[key] {
				return errors.New("key fields must be unique required schema fields")
			}
			keys[key] = true
		}
	}
	ids := map[string]bool{}
	for _, r := range records {
		if r.MutationID == "" || r.EventID == "" || r.Index < 0 || ids[r.MutationID] {
			return errors.New("missing or duplicate record identity")
		}
		ids[r.MutationID] = true
		m := r.Mutation
		o, ok := definitions[m.Output]
		if !ok {
			return fmt.Errorf("undeclared output %q", m.Output)
		}
		if m.Operation != "upsert" && m.Operation != "delete" {
			return errors.New("operation must be upsert or delete")
		}
		if len(m.Key) != len(o.KeyFields) {
			return errors.New("key must exactly match key_fields")
		}
		for _, key := range o.KeyFields {
			v, ok := m.Key[key]
			if !ok || v == nil {
				return errors.New("missing key field")
			}
			for _, f := range o.Fields {
				if f.Name == key {
					if e := checkValue(f.Type, v); e != nil {
						return fmt.Errorf("key %s: %w", key, e)
					}
				}
			}
		}
		if m.Operation == "delete" {
			if m.Value != nil {
				return errors.New("delete must not contain value")
			}
			continue
		}
		if m.Value == nil {
			return errors.New("upsert requires complete value")
		}
		fields := map[string]model.Column{}
		for _, f := range o.Fields {
			fields[f.Name] = f
			v, ok := m.Value[f.Name]
			if !ok || v == nil {
				if f.Required {
					return fmt.Errorf("required field %s missing or null", f.Name)
				}
				continue
			}
			if e := checkValue(f.Type, v); e != nil {
				return fmt.Errorf("field %s: %w", f.Name, e)
			}
		}
		for k := range m.Value {
			if _, ok := fields[k]; !ok {
				return fmt.Errorf("unknown field %s", k)
			}
		}
		for _, key := range o.KeyFields {
			if !reflect.DeepEqual(m.Key[key], m.Value[key]) {
				return fmt.Errorf("key/value mismatch for %s", key)
			}
		}
	}
	return nil
}

func validType(t string) error {
	switch t {
	case "string", "bool", "int64", "uint64", "float64":
		return nil
	}
	p, s, ok := decimalParts(t)
	if ok && p <= 38 && s <= p {
		return nil
	}
	return fmt.Errorf("unsupported type %q (decimal precision must be <= 38)", t)
}

func decimalParts(t string) (int, int, bool) {
	m := decimalType.FindStringSubmatch(t)
	if m == nil {
		return 0, 0, false
	}
	p, e := strconv.Atoi(m[1])
	if e != nil {
		return 0, 0, false
	}
	s, e := strconv.Atoi(m[2])
	return p, s, e == nil
}

func checkValue(t string, v any) error {
	switch t {
	case "string":
		if _, ok := v.(string); ok {
			return nil
		}
	case "bool":
		if _, ok := v.(bool); ok {
			return nil
		}
	case "int64", "uint64":
		s, ok := v.(string)
		if !ok {
			return errors.New("exact integer must be a string")
		}
		var e error
		if t == "int64" {
			_, e = strconv.ParseInt(s, 10, 64)
		} else {
			_, e = strconv.ParseUint(s, 10, 64)
		}
		if e == nil {
			return nil
		}
	case "float64":
		var n float64
		var e error
		switch x := v.(type) {
		case float64:
			n = x
		case json.Number:
			n, e = x.Float64()
		default:
			return errors.New("float64 must be numeric")
		}
		if e == nil && !math.IsNaN(n) && !math.IsInf(n, 0) {
			return nil
		}
	default:
		p, scale, ok := decimalParts(t)
		if !ok {
			return errors.New("unsupported type")
		}
		s, ok := v.(string)
		if !ok || !decimalValue.MatchString(s) {
			return errors.New("decimal must be a non-exponent decimal string")
		}
		parts := strings.Split(strings.TrimPrefix(s, "-"), ".")
		whole := len(strings.TrimLeft(parts[0], "0"))
		fraction := 0
		if len(parts) == 2 {
			fraction = len(parts[1])
		}
		if whole <= p-scale && fraction <= scale {
			return nil
		}
	}
	return fmt.Errorf("invalid %s value", t)
}
