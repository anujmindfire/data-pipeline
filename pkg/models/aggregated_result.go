/*
AggregatedResult represents final summarized statistics compiled at the end of a pipeline execution.
It tracks the total record count, sums, averages, and group-by category dictionaries.
*/
package models

type AggregatedResult struct {
	JobID       string                    `json:"job_id"`
	TotalCount  int64                     `json:"total_count"`
	Sums        map[string]float64        `json:"sums"`
	Averages    map[string]float64        `json:"averages"`
	GroupedData map[string]map[string]any `json:"grouped_data"`
}
