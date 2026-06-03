package pipeline

import (
	"context"
	"fmt"
	"math"
)

type Accumulator struct {
	Count    int64
	Sum      float64
	Min      float64
	Max      float64
	HasValue bool
}

// StartAggregationStage collects transformed records and computes final metrics.
func StartAggregationStage(ctx context.Context, jobSpec *JobSpec, transformedCh <-chan Record, exportRecordsCh chan<- Record, resultCh chan<- AggregatedResult, progressCh chan<- ProgressEvent) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(resultCh)
		defer close(exportRecordsCh)

		// Set up accumulators
		globalAccs := make(map[string]*Accumulator)
		groupAccs := make(map[string]map[string]*Accumulator) // target -> groupKey -> Accumulator

		// Initialize accumulators based on job specifications
		for _, spec := range jobSpec.AggregationTypes {
			if spec.GroupBy != "" {
				groupAccs[spec.Target] = make(map[string]*Accumulator)
			} else {
				globalAccs[spec.Target] = &Accumulator{
					Min: math.MaxFloat64,
					Max: -math.MaxFloat64,
				}
			}
		}

		recordCount := int64(0)

		for {
			select {
			case <-ctx.Done():
				return
			case record, ok := <-transformedCh:
				if !ok {
					// Input channel closed, compile final results
					results := compileFinalResults(jobSpec.ID, recordCount, jobSpec.AggregationTypes, globalAccs, groupAccs)
					
					// Send results to export stage
					select {
					case <-ctx.Done():
						return
					case resultCh <- results:
					}
					progressCh <- ProgressEvent{
						JobID:    jobSpec.ID,
						Stage:    "aggregation",
						Type:     ProgressStageCompleted,
						Delta:    0,
					}
					return
				}

				recordCount++

				// Apply aggregations
				for _, spec := range jobSpec.AggregationTypes {
					val, ok := record.ParsedData[spec.Field]
					if !ok || val == nil {
						continue // Skip if field is missing or null
					}

					fVal, err := parseFloat(val)
					if err != nil {
						continue // Skip if value cannot be parsed to float
					}

					if spec.GroupBy != "" {
						groupVal, ok := record.ParsedData[spec.GroupBy]
						if !ok || groupVal == nil {
							continue // Skip if group_by field is missing
						}
						groupKey := stringify(groupVal)

						accMap := groupAccs[spec.Target]
						acc, exists := accMap[groupKey]
						if !exists {
							acc = &Accumulator{
								Min: math.MaxFloat64,
								Max: -math.MaxFloat64,
							}
							accMap[groupKey] = acc
						}
						updateAccumulator(acc, fVal)
					} else {
						acc := globalAccs[spec.Target]
						updateAccumulator(acc, fVal)
					}
				}

				// Forward to export stage
				select {
				case <-ctx.Done():
					return
				case exportRecordsCh <- record:
				}

				// Report aggregation progress (counts toward processed records)
				progressCh <- ProgressEvent{
					JobID:    record.JobID,
					SourceID: record.SourceID,
					Stage:    "aggregate",
					Type:     ProgressRecordProcessed,
					Delta:    1,
				}
			}
		}
	}()
	return done
}

func updateAccumulator(acc *Accumulator, val float64) {
	acc.Count++
	acc.Sum += val
	if val < acc.Min {
		acc.Min = val
	}
	if val > acc.Max {
		acc.Max = val
	}
	acc.HasValue = true
}

func compileFinalResults(jobID string, recordCount int64, specs []AggregationSpec, global map[string]*Accumulator, group map[string]map[string]*Accumulator) AggregatedResult {
	sums := make(map[string]float64)
	averages := make(map[string]float64)
	groupedData := make(map[string]map[string]any)

	for _, spec := range specs {
		if spec.GroupBy != "" {
			// Compile Group-By aggregation
			accMap := group[spec.Target]
			groupResult := make(map[string]any)

			for groupKey, acc := range accMap {
				if !acc.HasValue {
					continue
				}
				groupResult[groupKey] = extractValue(spec.Func, acc)
			}
			groupedData[spec.Target] = groupResult
		} else {
			// Compile Global aggregation
			acc := global[spec.Target]
			if !acc.HasValue {
				continue
			}
			val := extractValue(spec.Func, acc)
			var f float64
			switch v := val.(type) {
			case float64:
				f = v
			case int64:
				f = float64(v)
			}
			if spec.Func == "avg" {
				averages[spec.Target] = f
			} else {
				sums[spec.Target] = f
			}
		}
	}

	return AggregatedResult{
		JobID:       jobID,
		TotalCount:  recordCount,
		Sums:        sums,
		Averages:    averages,
		GroupedData: groupedData,
	}
}

func extractValue(fn string, acc *Accumulator) any {
	switch fn {
	case "count":
		return acc.Count
	case "sum":
		return acc.Sum
	case "avg":
		if acc.Count == 0 {
			return 0.0
		}
		return acc.Sum / float64(acc.Count)
	case "min":
		if acc.Min == math.MaxFloat64 {
			return 0.0
		}
		return acc.Min
	case "max":
		if acc.Max == -math.MaxFloat64 {
			return 0.0
		}
		return acc.Max
	default:
		return 0.0
	}
}

func stringify(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	default:
		return fmt.Sprintf("%v", val)
	}
}

