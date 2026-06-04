package pipeline

import (
	"testing"

	"data-processing-pipeline/packages/shared/models"
)

func TestTransformationRules(t *testing.T) {
	rules := []models.TransformRuleSpec{
		{Field: "raw_number", Rule: "cast", Param: "float"},
		{Field: "raw_int", Rule: "cast", Param: "int"},
		{Field: "name", Rule: "lower"},
		{Field: "symbol", Rule: "upper"},
		{Field: "city", Rule: "trim"},
		{Field: "raw_number", Rule: "add_constant", Param: "10.5"},
		{Field: "processed_at", Rule: "enrich_time"},
	}

	transformers, err := compileTransformRules(rules)
	if err != nil {
		t.Fatalf("Failed to compile transform rules: %v", err)
	}

	payload := map[string]any{
		"raw_number": "100.5",
		"raw_int":    "42",
		"name":       "JOHN DOE",
		"symbol":     "btc",
		"city":       "   New York   ",
	}

	rec := models.Record{
		JobID:      "test-job",
		ParsedData: payload,
	}

	err = runTransformations(&rec, transformers)
	if err != nil {
		t.Fatalf("Transformations failed: %v", err)
	}

	// 1. Check float cast + constant addition (100.5 + 10.5 = 111.0)
	if f, ok := rec.ParsedData["raw_number"].(float64); !ok || f != 111.0 {
		t.Errorf("Expected raw_number to be 111.0, got %v", rec.ParsedData["raw_number"])
	}

	// 2. Check int cast
	if i, ok := rec.ParsedData["raw_int"].(int64); !ok || i != 42 {
		t.Errorf("Expected raw_int to be 42, got %v", rec.ParsedData["raw_int"])
	}

	// 3. Check lowercase
	if s, ok := rec.ParsedData["name"].(string); !ok || s != "john doe" {
		t.Errorf("Expected name to be 'john doe', got '%v'", rec.ParsedData["name"])
	}

	// 4. Check uppercase
	if s, ok := rec.ParsedData["symbol"].(string); !ok || s != "BTC" {
		t.Errorf("Expected symbol to be 'BTC', got '%v'", rec.ParsedData["symbol"])
	}

	// 5. Check trim
	if s, ok := rec.ParsedData["city"].(string); !ok || s != "New York" {
		t.Errorf("Expected city to be 'New York', got '%v'", rec.ParsedData["city"])
	}

	// 6. Check time enrichment
	if tStamp, ok := rec.ParsedData["processed_at"].(string); !ok || len(tStamp) == 0 {
		t.Errorf("Expected enriched processed_at timestamp string, got %v", rec.ParsedData["processed_at"])
	}
}
