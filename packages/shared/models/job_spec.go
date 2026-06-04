/*
JobSpec defines the configuration payload sent via the HTTP REST API to execute a pipeline run.
It parses and validates details like local paths or HTTP URLs (Sources), validation criteria (ValidationRules),
transformation mapping (TransformRules), mathematical summaries (AggregationTypes), and destination formats (ExportTargets).
It handles custom JSON marshaling to maintain backwards compatibility with legacy client parameters.
*/
package models

import (
	"encoding/json"
)

type JobSpec struct {
	ID               string                `json:"id"`
	Name             string                `json:"name"`
	Sources          []SourceSpec          `json:"sources"`
	ValidationRules  []ValidationRuleSpec  `json:"validation_rules"`
	TransformRules   []TransformRuleSpec   `json:"transform_rules"`
	AggregationTypes []AggregationSpec     `json:"aggregation_types"`
	ExportTargets    []ExportTargetSpec    `json:"export_targets"`
	WorkerPoolSizes  WorkerConfig          `json:"worker_pool_sizes"`
}

type WorkerConfig struct {
	Validation     int `json:"validation"`
	Transformation int `json:"transformation"`
}

type SourceSpec struct {
	ID     string            `json:"id"`
	Type   string            `json:"type"`
	Path   string            `json:"path"`
	Schema map[string]string `json:"schema"`
}

type ValidationRuleSpec struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
	Param string `json:"param"`
}

type TransformRuleSpec struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
	Param string `json:"param"`
}

type AggregationSpec struct {
	Field   string `json:"field"`
	Func    string `json:"func"`
	GroupBy string `json:"group_by,omitempty"`
	Target  string `json:"target"`
}

type ExportTargetSpec struct {
	Type string `json:"type"`
	Path string `json:"path"`
}

func (j *JobSpec) UnmarshalJSON(data []byte) error {
	type Alias JobSpec
	aux := &struct {
		*Alias
		AggregationSpecs []AggregationSpec `json:"aggregation_specs"`
		Workers          WorkerConfig      `json:"workers"`
	}{
		Alias: (*Alias)(j),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if len(j.AggregationTypes) == 0 && len(aux.AggregationSpecs) > 0 {
		j.AggregationTypes = aux.AggregationSpecs
	}
	if j.WorkerPoolSizes.Validation == 0 && j.WorkerPoolSizes.Transformation == 0 {
		if aux.Workers.Validation > 0 || aux.Workers.Transformation > 0 {
			j.WorkerPoolSizes = aux.Workers
		}
	}
	return nil
}

func (j *JobSpec) MarshalJSON() ([]byte, error) {
	type Alias JobSpec
	return json.Marshal(&struct {
		*Alias
		AggregationSpecs []AggregationSpec `json:"aggregation_specs,omitempty"`
		Workers          WorkerConfig      `json:"workers,omitempty"`
	}{
		Alias:            (*Alias)(j),
		AggregationSpecs: j.AggregationTypes,
		Workers:          j.WorkerPoolSizes,
	})
}
