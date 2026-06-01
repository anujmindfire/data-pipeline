package pipeline

import (
	"testing"
)

func TestValidationRules(t *testing.T) {
	rules := []ValidationRuleSpec{
		{Field: "country", Rule: "required"},
		{Field: "cases", Rule: "min", Param: "0"},
		{Field: "deaths", Rule: "max", Param: "100"},
		{Field: "cases", Rule: "type", Param: "float"},
		{Field: "postal_code", Rule: "regex", Param: "^[0-9]{5}$"},
	}

	validators, err := compileValidationRules(rules)
	if err != nil {
		t.Fatalf("Failed to compile validation rules: %v", err)
	}

	tests := []struct {
		name    string
		payload map[string]any
		isValid bool
	}{
		{
			name: "Valid Record",
			payload: map[string]any{
				"country":     "Canada",
				"cases":       15.5,
				"deaths":      5,
				"postal_code": "12345",
			},
			isValid: true,
		},
		{
			name: "Missing Required Field",
			payload: map[string]any{
				"cases":       10,
				"deaths":      5,
				"postal_code": "12345",
			},
			isValid: false,
		},
		{
			name: "Below Min limit",
			payload: map[string]any{
				"country":     "Canada",
				"cases":       -1,
				"deaths":      5,
				"postal_code": "12345",
			},
			isValid: false,
		},
		{
			name: "Above Max limit",
			payload: map[string]any{
				"country":     "Canada",
				"cases":       10,
				"deaths":      150,
				"postal_code": "12345",
			},
			isValid: false,
		},
		{
			name: "Regex Mismatch",
			payload: map[string]any{
				"country":     "Canada",
				"cases":       10,
				"deaths":      5,
				"postal_code": "123-45",
			},
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := Record{
				JobID:    "test-job",
				SourceID: "test-src",
				RecordID: "rec-1",
				Payload:  tt.payload,
			}
			errs := runValidation(rec, validators)
			if tt.isValid && len(errs) > 0 {
				t.Errorf("Expected record to be valid, but got errors: %v", errs)
			}
			if !tt.isValid && len(errs) == 0 {
				t.Errorf("Expected record to fail validation, but got no errors")
			}
		})
	}
}
