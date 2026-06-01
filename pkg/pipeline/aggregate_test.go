package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestAggregationRules(t *testing.T) {
	spec := &JobSpec{
		AggregationSpecs: []AggregationSpec{
			{Field: "score", Func: "sum", Target: "total_score"},
			{Field: "score", Func: "avg", Target: "avg_score"},
			{Field: "score", Func: "min", Target: "min_score"},
			{Field: "score", Func: "max", Target: "max_score"},
			{Field: "score", Func: "count", Target: "count_score"},
			// Group By
			{Field: "score", Func: "sum", GroupBy: "category", Target: "sum_by_category"},
		},
	}

	transformedCh := make(chan Record, 5)
	exportRecordsCh := make(chan Record, 5)
	resultCh := make(chan map[string]any, 1)
	progressCh := make(chan ProgressEvent, 10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start aggregation in background
	StartAggregationStage(ctx, spec, transformedCh, exportRecordsCh, resultCh, progressCh)

	// Feed test records
	records := []map[string]any{
		{"score": 10.0, "category": "A"},
		{"score": 20.0, "category": "A"},
		{"score": 5.0, "category": "B"},
		{"score": 15.0, "category": "B"},
	}

	for i, r := range records {
		transformedCh <- Record{
			JobID:    "test-job",
			SourceID: "src-1",
			RecordID: stringify(i),
			Payload:  r,
		}
	}
	close(transformedCh)

	// Drain export records to prevent blocking
	go func() {
		for range exportRecordsCh {}
	}()
	// Drain progress to prevent blocking
	go func() {
		for range progressCh {}
	}()

	// Wait for results
	var results map[string]any
	select {
	case res, ok := <-resultCh:
		if !ok {
			t.Fatalf("results channel closed prematurely")
		}
		results = res
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for aggregation results")
	}

	// 1. Verify global sum (10 + 20 + 5 + 15 = 50.0)
	if val, ok := results["total_score"].(float64); !ok || val != 50.0 {
		t.Errorf("Expected total_score 50.0, got %v", results["total_score"])
	}

	// 2. Verify global average (50 / 4 = 12.5)
	if val, ok := results["avg_score"].(float64); !ok || val != 12.5 {
		t.Errorf("Expected avg_score 12.5, got %v", results["avg_score"])
	}

	// 3. Verify global min (5.0)
	if val, ok := results["min_score"].(float64); !ok || val != 5.0 {
		t.Errorf("Expected min_score 5.0, got %v", results["min_score"])
	}

	// 4. Verify global max (20.0)
	if val, ok := results["max_score"].(float64); !ok || val != 20.0 {
		t.Errorf("Expected max_score 20.0, got %v", results["max_score"])
	}

	// 5. Verify global count (4)
	if val, ok := results["count_score"].(int64); !ok || val != 4 {
		t.Errorf("Expected count_score 4, got %v", results["count_score"])
	}

	// 6. Verify Group By Sum
	groupBySum, ok := results["sum_by_category"].(map[string]any)
	if !ok {
		t.Fatalf("Expected sum_by_category to be a map, got %T", results["sum_by_category"])
	}

	if val, ok := groupBySum["A"].(float64); !ok || val != 30.0 {
		t.Errorf("Expected sum for Category A to be 30.0, got %v", groupBySum["A"])
	}

	if val, ok := groupBySum["B"].(float64); !ok || val != 20.0 {
		t.Errorf("Expected sum for Category B to be 20.0, got %v", groupBySum["B"])
	}
}
