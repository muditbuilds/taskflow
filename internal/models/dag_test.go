package models

import (
	"encoding/json"
	"testing"
)

func TestParseDag(t *testing.T) {
	def := DagDefinition{
		Steps: []DagStep{
			{ID: "a", Type: "t1", Payload: json.RawMessage(`{}`)},
			{ID: "b", Type: "t2", Payload: json.RawMessage(`{}`)},
			{ID: "c", Type: "t3", Payload: json.RawMessage(`{}`)},
		},
		Edges: []DagEdge{{From: "a", To: "b"}, {From: "b", To: "c"}},
	}
	parsed, err := ParseDag(def)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Ready) != 1 {
		t.Errorf("expected 1 root, got %d", len(parsed.Ready))
	}
	if _, ok := parsed.Ready["a"]; !ok {
		t.Error("expected root a")
	}
	if len(parsed.Parents["b"]) != 1 || parsed.Parents["b"][0] != "a" {
		t.Errorf("expected b's parent a, got %v", parsed.Parents["b"])
	}
	if len(parsed.Parents["c"]) != 1 || parsed.Parents["c"][0] != "b" {
		t.Errorf("expected c's parent b, got %v", parsed.Parents["c"])
	}
}

func TestParsedDag_ReadyToRun(t *testing.T) {
	def := DagDefinition{
		Steps: []DagStep{
			{ID: "a", Type: "t1", Payload: json.RawMessage(`{}`)},
			{ID: "b", Type: "t2", Payload: json.RawMessage(`{}`)},
			{ID: "c", Type: "t3", Payload: json.RawMessage(`{}`)},
		},
		Edges: []DagEdge{{From: "a", To: "b"}, {From: "b", To: "c"}},
	}
	parsed, _ := ParseDag(def)

	ready := parsed.ReadyToRun(nil)
	if len(ready) != 1 || ready[0] != "a" {
		t.Errorf("expected [a], got %v", ready)
	}

	ready = parsed.ReadyToRun([]string{"a"})
	if len(ready) != 1 || ready[0] != "b" {
		t.Errorf("expected [b] after a done, got %v", ready)
	}

	ready = parsed.ReadyToRun([]string{"a", "b"})
	if len(ready) != 1 || ready[0] != "c" {
		t.Errorf("expected [c] after a,b done, got %v", ready)
	}

	ready = parsed.ReadyToRun([]string{"a", "b", "c"})
	if len(ready) != 0 {
		t.Errorf("expected none when all done, got %v", ready)
	}
}

func TestParseDag_MultipleRoots(t *testing.T) {
	def := DagDefinition{
		Steps: []DagStep{
			{ID: "a", Type: "t1", Payload: json.RawMessage(`{}`)},
			{ID: "b", Type: "t2", Payload: json.RawMessage(`{}`)},
			{ID: "c", Type: "t3", Payload: json.RawMessage(`{}`)},
		},
		Edges: []DagEdge{{From: "a", To: "c"}, {From: "b", To: "c"}},
	}
	parsed, err := ParseDag(def)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Ready) != 2 {
		t.Errorf("expected 2 roots (a,b), got %d", len(parsed.Ready))
	}
	ready := parsed.ReadyToRun(nil)
	if len(ready) != 2 {
		t.Errorf("expected [a b], got %v", ready)
	}
	ready = parsed.ReadyToRun([]string{"a", "b"})
	if len(ready) != 1 || ready[0] != "c" {
		t.Errorf("expected [c], got %v", ready)
	}
}
