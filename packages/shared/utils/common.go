package utils

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseFloat converts a generic interface value to float64, supporting string parsing and cleaning.
func ParseFloat(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		clean := strings.TrimSpace(v)
		if clean == "" || clean == "null" || clean == "N/A" {
			return 0, nil
		}
		return strconv.ParseFloat(clean, 64)
	default:
		return 0, fmt.Errorf("unable to convert type %T to float", val)
	}
}
