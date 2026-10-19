package luaruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"example.com/etl/internal/model"
)

func event() model.Event {
	return model.Event{ID: "test-event", Operation: "insert", After: map[string]any{"id": "9007199254740993", "price": "12.30", "quantity": "3", "optional": nil}}
}

func TestTransformMappingAndNull(t *testing.T) {
	source := `function transform(e)
  assert(os == nil and io == nil and package == nil and debug == nil)
  assert(loadfile == nil and dofile == nil and loadstring == nil and print == nil)
  assert(math.random == nil and math.randomseed == nil)
  assert(e.after.optional == etl.null)
  return {{output="orders", op="upsert", key={id=e.after.id},
    value={id=e.after.id,total=etl.decimal_mul(e.after.price,e.after.quantity), optional=e.after.optional}}}
end`
	res, err := Execute(context.Background(), model.TransformRequest{Source: source, Events: []model.Event{event()}})
	if err != nil {
		t.Fatal(err)
	}
	m := res.Results[0][0]
	if m.Key["id"] != "9007199254740993" || m.Value["total"] != "36.9" {
		t.Fatalf("wrong result: %#v", m)
	}
	if v, ok := m.Value["optional"]; !ok || v != nil {
		t.Fatalf("null was lost: %#v", m.Value)
	}
}

func TestUpdateDeleteFilterAndSplit(t *testing.T) {
	source := `function transform(e)
  if e.op == "delete" then return {{output="orders",op="delete",key={id=e.before.id}}} end
  if e.after.skip then return {} end
  return {
    {output="orders",op="upsert",key={id=e.after.id},value=e.after},
    {output="audit",op="upsert",key={id=e.after.id},value={id=e.after.id}}
  }
end`
	update := event()
	update.Operation = "update"
	update.Before = map[string]any{"id": "old"}
	del := event()
	del.Operation = "delete"
	del.Before = map[string]any{"id": "deleted"}
	del.After = nil
	filter := event()
	filter.After["skip"] = true
	res, err := Execute(context.Background(), model.TransformRequest{Source: source, Events: []model.Event{update, del, filter}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Results) != 3 || len(res.Results[0]) != 2 || len(res.Results[1]) != 1 || len(res.Results[2]) != 0 {
		t.Fatalf("bad event correlation: %#v", res)
	}
	if res.Results[1][0].Operation != "delete" || res.Results[1][0].Key["id"] != "deleted" {
		t.Fatal("lost deletion")
	}
}

func TestInvalidReturns(t *testing.T) {
	for name, body := range map[string]string{
		"nil":           "return nil",
		"sparse":        "return {[2]={}}",
		"not-array":     "return {output='orders'}",
		"non-object":    "return {1}",
		"bad-op":        "return {{output='orders',op='insert',key={id='1'},value={}}}",
		"missing-key":   "return {{output='orders',op='delete'}}",
		"delete-value":  "return {{output='orders',op='delete',key={id='1'},value={}}}",
		"cycle":         "local x={}; x.self=x; return {{output='orders',op='upsert',key={id='1'},value=x}}",
		"nan":           "return {{output='orders',op='upsert',key={id='1'},value={a=0/0}}}",
		"mixed-table":   "return {{output='orders',op='upsert',key={id='1'},value={a={1,a=2}}}}",
		"function":      "return {{output='orders',op='upsert',key={id='1'},value={a=function()end}}}",
		"sparse-nested": "return {{output='orders',op='upsert',key={id='1'},value={a={[3]='a'}}}}",
		"unknown-field": "return {{output='orders',op='delete',key={id='1'},unknown=true}}",
		"huge-decimal":  "return {{output='orders',op='upsert',key={id='1'},value={a=etl.decimal_add('1e2147483647','1')}}}",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Execute(context.Background(), model.TransformRequest{Source: "function transform(e) " + body + " end", Events: []model.Event{event()}})
			if err == nil {
				t.Fatal("accepted invalid result")
			}
		})
	}
}

func TestFreshVMPerEvent(t *testing.T) {
	source := `local count=0; function transform(e) count=count+1; return {{output="orders",op="upsert",key={id=e.after.id},value={count=count}}} end`
	res, err := Execute(context.Background(), model.TransformRequest{Source: source, Events: []model.Event{event(), event()}})
	if err != nil {
		t.Fatal(err)
	}
	for _, mutations := range res.Results {
		if mutations[0].Value["count"] != float64(1) {
			t.Fatal("VM state leaked")
		}
	}
}

func TestInfiniteLoopCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Execute(ctx, model.TransformRequest{Source: "function transform(e) while true do end end", Events: []model.Event{event()}})
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("loop not cancelled: %v", err)
	}
}

func TestValidateBoundedInitialization(t *testing.T) {
	if err := Validate("function transform(e) return {} end"); err != nil {
		t.Fatal(err)
	}
	if err := Validate("function other(e) end"); err == nil {
		t.Fatal("missing entrypoint accepted")
	}
	if err := Validate("while true do end"); err == nil {
		t.Fatal("infinite initialization accepted")
	}
}

func TestWorkerRoundtripAndLimits(t *testing.T) {
	req := model.TransformRequest{Source: "function transform(e) return {} end", Events: []model.Event{event()}}
	data, _ := json.Marshal(req)
	var out bytes.Buffer
	if err := RunWorker(bytes.NewReader(data), &out); err != nil {
		t.Fatal(err)
	}
	var res model.TransformResponse
	if err := json.Unmarshal(out.Bytes(), &res); err != nil || res.Error != "" || len(res.Results) != 1 {
		t.Fatalf("bad worker response: %s, %v", out.String(), err)
	}
	if err := RunWorker(strings.NewReader(strings.Repeat(" ", MaxRequestBytes+1)), &out); err == nil {
		t.Fatal("oversized request accepted")
	}
	if err := RunWorker(strings.NewReader(string(data)+" {}"), &out); err == nil {
		t.Fatal("trailing request accepted")
	}
	_, err := Execute(context.Background(), model.TransformRequest{Source: strings.Repeat(" ", MaxRequestBytes+1)})
	if err == nil {
		t.Fatal("Execute accepted oversized request")
	}
}

func TestResponseLimit(t *testing.T) {
	_, err := Execute(context.Background(), model.TransformRequest{
		Source: `function transform(e)
  return {{output="orders",op="upsert",key={id="1"},value={payload=string.rep("x",5*1024*1024)}}}
end`,
		Events: []model.Event{event()},
	})
	if err == nil || !strings.Contains(err.Error(), "response exceeds") {
		t.Fatalf("expected output size rejection, got %v", err)
	}
}

// The subprocess test harness dispatches exactly like cmd/etl lua-worker.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "lua-worker" {
		if err := RunWorker(os.Stdin, os.Stdout); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestProcess(t *testing.T) {
	res, err := (Process{}).Execute(context.Background(), model.TransformRequest{
		Source: "function transform(e) return {} end", Events: []model.Event{event()},
	})
	if err != nil || len(res.Results) != 1 {
		t.Fatalf("process failed: %#v %v", res, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = (Process{}).Execute(ctx, model.TransformRequest{Source: "while true do end", Events: []model.Event{event()}})
	if err == nil {
		t.Fatal("infinite worker not terminated")
	}
}
