// Package luaruntime executes the MVP's pure Lua transforms. A subprocess limits
// execution lifetime, but does not provide a hard memory or security sandbox.
package luaruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"regexp"
	"time"

	"example.com/etl/internal/model"
	"github.com/shopspring/decimal"
	lua "github.com/yuin/gopher-lua"
)

const (
	MaxRequestBytes  = 1 << 20
	MaxResponseBytes = 4 << 20
	ExecutionTimeout = 2 * time.Second
)

var decimalPattern = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?$`)

func newState(ctx context.Context) (*lua.LState, *lua.LUserData) {
	l := lua.NewState(lua.Options{SkipOpenLibs: true})
	l.SetContext(ctx)
	lua.OpenBase(l)
	allowed := map[string]bool{"assert": true, "error": true, "ipairs": true, "next": true, "pairs": true, "pcall": true, "select": true, "tonumber": true, "tostring": true, "type": true, "unpack": true, "xpcall": true, "_VERSION": true}
	var remove []string
	l.G.Global.ForEach(func(k, _ lua.LValue) {
		if !allowed[k.String()] {
			remove = append(remove, k.String())
		}
	})
	for _, key := range remove {
		l.SetGlobal(key, lua.LNil)
	}
	lua.OpenString(l)
	lua.OpenTable(l)
	lua.OpenMath(l)
	mt := l.GetGlobal("math").(*lua.LTable)
	mt.RawSetString("random", lua.LNil)
	mt.RawSetString("randomseed", lua.LNil)
	null := l.NewUserData()
	null.Value = "etl.null"
	etl := l.NewTable()
	etl.RawSetString("null", null)
	decimalOp := func(mul bool) lua.LGFunction {
		return func(l *lua.LState) int {
			if l.Get(1).Type() != lua.LTString || l.Get(2).Type() != lua.LTString {
				l.RaiseError("decimal operands must be strings")
				return 0
			}
			for i := 1; i <= 2; i++ {
				s := l.CheckString(i)
				if len(s) > 128 || !decimalPattern.MatchString(s) {
					l.RaiseError("decimal operands must be plain decimal strings of at most 128 bytes")
					return 0
				}
			}
			a, err := decimal.NewFromString(l.CheckString(1))
			if err != nil {
				l.RaiseError("invalid decimal operand")
				return 0
			}
			b, err := decimal.NewFromString(l.CheckString(2))
			if err != nil {
				l.RaiseError("invalid decimal operand")
				return 0
			}
			if mul {
				a = a.Mul(b)
			} else {
				a = a.Add(b)
			}
			l.Push(lua.LString(a.String()))
			return 1
		}
	}
	etl.RawSetString("decimal_add", l.NewFunction(decimalOp(false)))
	etl.RawSetString("decimal_mul", l.NewFunction(decimalOp(true)))
	l.SetGlobal("etl", etl)
	return l, null
}

// Validate compiles and runs initialization with a deadline, then verifies the
// entrypoint. Runtime output validation happens when real events are supplied.
func Validate(source string) error {
	if len(source) > MaxRequestBytes {
		return errors.New("Lua source exceeds 1 MiB")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	l, _ := newState(ctx)
	defer l.Close()
	if err := l.DoString(source); err != nil {
		return fmt.Errorf("Lua initialization: %w", err)
	}
	if l.GetGlobal("transform").Type() != lua.LTFunction {
		return errors.New("Lua source must define function transform(event)")
	}
	return nil
}

// Execute gives each event its own VM, so globals cannot carry state from a
// preceding event. The deadline covers initialization and all events together.
func Execute(ctx context.Context, req model.TransformRequest) (model.TransformResponse, error) {
	input, err := json.Marshal(req)
	if err != nil {
		return model.TransformResponse{}, err
	}
	if len(input) > MaxRequestBytes {
		return model.TransformResponse{}, errors.New("transform request exceeds 1 MiB")
	}
	ctx, cancel := context.WithTimeout(ctx, ExecutionTimeout)
	defer cancel()
	res := model.TransformResponse{Results: make([][]model.Mutation, 0, len(req.Events))}
	if len(req.Events) == 0 {
		if err := Validate(req.Source); err != nil {
			return model.TransformResponse{}, err
		}
		return res, nil
	}
	for _, event := range req.Events {
		mutations, err := executeEvent(ctx, req.Source, event)
		if err != nil {
			return model.TransformResponse{}, fmt.Errorf("event %s: %w", event.ID, err)
		}
		res.Results = append(res.Results, mutations)
	}
	encoded, err := json.Marshal(res)
	if err != nil {
		return model.TransformResponse{}, err
	}
	if len(encoded) > MaxResponseBytes {
		return model.TransformResponse{}, errors.New("transform response exceeds 4 MiB")
	}
	return res, nil
}

func executeEvent(ctx context.Context, source string, event model.Event) ([]model.Mutation, error) {
	l, null := newState(ctx)
	defer l.Close()
	if err := l.DoString(source); err != nil {
		return nil, fmt.Errorf("Lua initialization: %w", err)
	}
	fn := l.GetGlobal("transform")
	if fn.Type() != lua.LTFunction {
		return nil, errors.New("missing transform function")
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	var raw any
	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	arg, err := toLua(l, null, raw)
	if err != nil {
		return nil, err
	}
	if err := l.CallByParam(lua.P{Fn: fn, NRet: 1, Protect: true}, arg); err != nil {
		return nil, fmt.Errorf("Lua transform: %w", err)
	}
	result, ok := l.Get(-1).(*lua.LTable)
	if !ok {
		return nil, errors.New("transform must return a continuous array of mutations ({} filters an event)")
	}
	length, err := arrayLength(result)
	if err != nil {
		return nil, fmt.Errorf("transform result: %w", err)
	}
	mutations := make([]model.Mutation, 0, length)
	for i := 1; i <= length; i++ {
		value, err := fromLua(result.RawGetInt(i), null, map[*lua.LTable]bool{}, 0)
		if err != nil {
			return nil, fmt.Errorf("mutation %d: %w", i, err)
		}
		obj, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mutation %d must be an object", i)
		}
		for key := range obj {
			if key != "output" && key != "op" && key != "key" && key != "value" {
				return nil, fmt.Errorf("unknown mutation field %q", key)
			}
		}
		output, _ := obj["output"].(string)
		op, _ := obj["op"].(string)
		key, keyOK := obj["key"].(map[string]any)
		if output == "" || (op != "upsert" && op != "delete") || !keyOK || len(key) == 0 {
			return nil, fmt.Errorf("mutation %d requires output, op=upsert|delete and a nonempty key object", i)
		}
		row, rowOK := obj["value"].(map[string]any)
		if op == "upsert" && !rowOK {
			return nil, fmt.Errorf("mutation %d upsert requires a value object", i)
		}
		if op == "delete" {
			if _, exists := obj["value"]; exists {
				return nil, fmt.Errorf("mutation %d delete must omit value", i)
			}
		}
		mutations = append(mutations, model.Mutation{Output: output, Operation: op, Key: key, Value: row})
	}
	return mutations, nil
}

func toLua(l *lua.LState, null *lua.LUserData, v any) (lua.LValue, error) {
	switch v := v.(type) {
	case nil:
		return null, nil
	case string:
		return lua.LString(v), nil
	case bool:
		return lua.LBool(v), nil
	case json.Number:
		n, err := v.Float64()
		if err != nil {
			return nil, err
		}
		return lua.LNumber(n), nil
	case map[string]any:
		t := l.NewTable()
		for k, x := range v {
			y, err := toLua(l, null, x)
			if err != nil {
				return nil, err
			}
			t.RawSetString(k, y)
		}
		return t, nil
	case []any:
		t := l.NewTable()
		for _, x := range v {
			y, err := toLua(l, null, x)
			if err != nil {
				return nil, err
			}
			t.Append(y)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("unsupported input type %T", v)
	}
}

func arrayLength(t *lua.LTable) (int, error) {
	count, max := 0, 0
	var invalid bool
	t.ForEach(func(k, _ lua.LValue) {
		n, ok := k.(lua.LNumber)
		if !ok || float64(n) < 1 || math.Trunc(float64(n)) != float64(n) || float64(n) > MaxResponseBytes {
			invalid = true
			return
		}
		count++
		if int(n) > max {
			max = int(n)
		}
	})
	if invalid || count != max {
		return 0, errors.New("array keys must be consecutive integers starting at 1")
	}
	return count, nil
}

func fromLua(v lua.LValue, null *lua.LUserData, seen map[*lua.LTable]bool, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("output nesting exceeds 64 levels")
	}
	if v == null {
		return nil, nil
	}
	switch v := v.(type) {
	case lua.LString:
		return string(v), nil
	case lua.LBool:
		return bool(v), nil
	case lua.LNumber:
		f := float64(v)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, errors.New("non-finite number in output")
		}
		return f, nil
	case *lua.LTable:
		if seen[v] {
			return nil, errors.New("cyclic table in output")
		}
		seen[v] = true
		defer delete(seen, v)
		hasNumber, hasString, bad := false, false, false
		v.ForEach(func(k, _ lua.LValue) {
			switch k.(type) {
			case lua.LNumber:
				hasNumber = true
			case lua.LString:
				hasString = true
			default:
				bad = true
			}
		})
		if bad || (hasNumber && hasString) {
			return nil, errors.New("output table must be an object or continuous array")
		}
		if hasNumber {
			n, err := arrayLength(v)
			if err != nil {
				return nil, err
			}
			a := make([]any, 0, n)
			for i := 1; i <= n; i++ {
				x, err := fromLua(v.RawGetInt(i), null, seen, depth+1)
				if err != nil {
					return nil, err
				}
				a = append(a, x)
			}
			return a, nil
		}
		obj := map[string]any{}
		var convErr error
		v.ForEach(func(k, x lua.LValue) {
			if convErr != nil {
				return
			}
			var y any
			y, convErr = fromLua(x, null, seen, depth+1)
			obj[string(k.(lua.LString))] = y
		})
		return obj, convErr
	default:
		return nil, fmt.Errorf("unsupported Lua output type %s", v.Type())
	}
}

// RunWorker handles exactly one bounded JSON request. Main should route the
// lua-worker command here before loading credentials or starting other services.
func RunWorker(in io.Reader, out io.Writer) error {
	data, err := io.ReadAll(io.LimitReader(in, MaxRequestBytes+1))
	if err != nil {
		return err
	}
	if len(data) > MaxRequestBytes {
		return errors.New("transform request exceeds 1 MiB")
	}
	var req model.TransformRequest
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("expected exactly one request")
	}
	res, err := Execute(context.Background(), req)
	if err != nil {
		res = model.TransformResponse{Error: err.Error()}
	}
	data, err = json.Marshal(res)
	if err != nil {
		return err
	}
	if len(data) > MaxResponseBytes {
		return errors.New("transform response exceeds 4 MiB")
	}
	_, err = out.Write(data)
	return err
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, errors.New("worker output limit exceeded")
	}
	return b.Buffer.Write(p)
}

// Process executes the same binary's lua-worker subcommand without inheriting
// host credentials. There is no hard process-memory limit in the local MVP.
type Process struct{ Executable string }

func (p Process) Execute(ctx context.Context, req model.TransformRequest) (model.TransformResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return model.TransformResponse{}, err
	}
	if len(data) > MaxRequestBytes {
		return model.TransformResponse{}, errors.New("transform request exceeds 1 MiB")
	}
	path := p.Executable
	if path == "" {
		path, err = os.Executable()
		if err != nil {
			return model.TransformResponse{}, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "lua-worker")
	cmd.Env = []string{}
	cmd.Stdin = bytes.NewReader(data)
	stdout := &boundedBuffer{limit: MaxResponseBytes}
	stderr := &boundedBuffer{limit: 4096}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return model.TransformResponse{}, fmt.Errorf("Lua worker: %w", ctx.Err())
		}
		return model.TransformResponse{}, fmt.Errorf("Lua worker: %w: %s", err, stderr.String())
	}
	var res model.TransformResponse
	dec := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&res); err != nil {
		return model.TransformResponse{}, fmt.Errorf("Lua worker response: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return model.TransformResponse{}, errors.New("unexpected trailing worker response")
	}
	if res.Error != "" {
		return model.TransformResponse{}, errors.New(res.Error)
	}
	if len(res.Results) != len(req.Events) {
		return model.TransformResponse{}, errors.New("Lua worker returned wrong event count")
	}
	return res, nil
}
