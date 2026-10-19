package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// DecodeJSON rejects unknown keys, duplicate keys, deeply nested documents,
// oversized definitions and multiple documents. A general JSON Schema validator
// is not implemented; Compile performs the v1 structural/graph checks.
func DecodeJSON(data []byte) (Definition, error) {
	if err := ValidateJSON(data); err != nil {
		return Definition{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var d Definition
	if err := decoder.Decode(&d); err != nil {
		return Definition{}, err
	}
	return d, nil
}

func scanValue(d *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("definition nesting exceeds 64")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			token, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok {
				return errors.New("object key must be string")
			}
			if seen[key] {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = true
			if err := scanValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := scanValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}

// ValidateJSON checks framing, duplicate keys and resource limits for API JSON.
func ValidateJSON(data []byte) error {
	if len(data) > 1<<20 {
		return errors.New("definition exceeds 1 MiB")
	}
	scanner := json.NewDecoder(bytes.NewReader(data))
	scanner.UseNumber()
	if err := scanValue(scanner, 0); err != nil {
		return err
	}
	if _, err := scanner.Token(); err != io.EOF {
		return errors.New("expected exactly one JSON document")
	}
	return nil
}
