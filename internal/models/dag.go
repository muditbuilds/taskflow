package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type DagRunStatus string

const (
	DagRunStatusPending   DagRunStatus = "pending"
	DagRunStatusRunning   DagRunStatus = "running"
	DagRunStatusCompleted DagRunStatus = "completed"
	DagRunStatusFailed    DagRunStatus = "failed"
	DagRunStatusCancelled DagRunStatus = "cancelled"
)

// DagDefinition is the JSON payload for submitting a DAG.
type DagDefinition struct {
	Steps []DagStep `json:"steps"`
	Edges []DagEdge `json:"edges"` // from -> to step IDs
}

type DagStep struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Priority int             `json:"priority,omitempty"`
}

type DagEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// DagRun is a single execution of a DAG.
type DagRun struct {
	ID         uuid.UUID       `json:"id"`
	Definition json.RawMessage `json:"definition"`
	Status     DagRunStatus    `json:"status"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// ParsedDag holds in-memory representation for scheduling.
type ParsedDag struct {
	Steps     []DagStep
	Edges     []DagEdge
	StepByID  map[string]DagStep
	Children  map[string][]string // step ID -> child step IDs
	Parents   map[string][]string // step ID -> parent step IDs
	Ready     map[string]struct{} // steps with no pending parents
	Completed map[string]struct{}
}

func ParseDag(def DagDefinition) (*ParsedDag, error) {
	stepByID := make(map[string]DagStep)
	for _, s := range def.Steps {
		stepByID[s.ID] = s
	}
	children := make(map[string][]string)
	parents := make(map[string][]string)
	for _, e := range def.Edges {
		children[e.From] = append(children[e.From], e.To)
		parents[e.To] = append(parents[e.To], e.From)
	}
	ready := make(map[string]struct{})
	for _, s := range def.Steps {
		if len(parents[s.ID]) == 0 {
			ready[s.ID] = struct{}{}
		}
	}
	return &ParsedDag{
		Steps:     def.Steps,
		Edges:     def.Edges,
		StepByID:  stepByID,
		Children:  children,
		Parents:   parents,
		Ready:     ready,
		Completed: make(map[string]struct{}),
	}, nil
}

// ReadyToRun returns step IDs that have all parents completed and are not yet completed.
func (p *ParsedDag) ReadyToRun(completed []string) []string {
	done := make(map[string]struct{})
	for _, s := range completed {
		done[s] = struct{}{}
	}
	var out []string
	for stepID := range p.StepByID {
		if _, ok := done[stepID]; ok {
			continue
		}
		prs := p.Parents[stepID]
		allDone := true
		for _, pr := range prs {
			if _, ok := done[pr]; !ok {
				allDone = false
				break
			}
		}
		if allDone {
			out = append(out, stepID)
		}
	}
	return out
}
