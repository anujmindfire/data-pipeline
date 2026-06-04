package pipeline

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"data-processing-pipeline/packages/shared/models"
)

const (
	ProgressRecordFailed ProgressEventType = "record_failed"
)

// StartValidationStage spawns worker goroutines to validate records in parallel.
func StartValidationStage(ctx context.Context, jobSpec *models.JobSpec, recordsCh <-chan models.Record, validatedCh chan<- models.Record, errorCh chan<- ErrorEvent, progressCh chan<- ProgressEvent) <-chan struct{} {
	done := make(chan struct{})
	var wg sync.WaitGroup
	numWorkers := jobSpec.WorkerPoolSizes.Validation
	if numWorkers <= 0 {
		numWorkers = 1
	}

	// Pre-compile validation rules for maximum throughput
	compiledValidators, compileErr := compileValidationRules(jobSpec.ValidationRules)
	if compileErr != nil {
		// If rule compilation fails, report error and exit
		errorCh <- ErrorEvent{
			JobID:        jobSpec.ID,
			Stage:        "validation",
			ErrorMessage: fmt.Sprintf("failed to pre-compile validation rules: %v", compileErr),
		}
		close(validatedCh)
		close(done)
		return done
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case record, ok := <-recordsCh:
					if !ok {
						return
					}

					errs := runValidation(record, compiledValidators)
					if len(errs) > 0 {
						// Record is invalid, send error events
						record.IsValid = false
						record.Errors = append(record.Errors, errs...)
						
						for _, errMsg := range errs {
							errorCh <- ErrorEvent{
								JobID:        record.JobID,
								Stage:        "validation",
								RawData:      string(record.RawPayload),
								ErrorMessage: errMsg,
							}
						}

						// Track failure metric
						progressCh <- ProgressEvent{
							JobID:    record.JobID,
							SourceID: record.SourceID,
							Stage:    "validation",
							Type:     ProgressRecordFailed,
							Delta:    1,
						}
					} else {
						// Record is valid, send to next stage
						select {
						case <-ctx.Done():
							return
						case validatedCh <- record:
						}
					}
				}
			}
		}(i)
	}

	// Fan-in: Wait for workers to complete and close output channel
	go func() {
		wg.Wait()
		close(validatedCh)
		progressCh <- ProgressEvent{
			JobID:    jobSpec.ID,
			Stage:    "validation",
			Type:     ProgressStageCompleted,
			Delta:    0,
		}
		close(done)
	}()
	return done
}

type compiledValidator struct {
	field string
	check func(val interface{}) error
}

func compileValidationRules(rules []models.ValidationRuleSpec) ([]compiledValidator, error) {
	validators := make([]compiledValidator, 0, len(rules))

	for _, spec := range rules {
		ruleType := spec.Rule
		field := spec.Field
		param := spec.Param

		var check func(val interface{}) error

		switch ruleType {
		case "required":
			check = func(val interface{}) error {
				if val == nil {
					return fmt.Errorf("field '%s' is required but missing", field)
				}
				if str, ok := val.(string); ok && len(str) == 0 {
					return fmt.Errorf("field '%s' is required but empty", field)
				}
				return nil
			}

		case "min":
			minVal, err := parseFloat(param)
			if err != nil {
				return nil, fmt.Errorf("invalid parameter '%s' for rule 'min' on field '%s': %w", param, field, err)
			}
			check = func(val interface{}) error {
				if val == nil {
					return nil // Assume optional if checked via required separately
				}
				fVal, err := parseFloat(val)
				if err != nil {
					return fmt.Errorf("field '%s' value %v cannot be parsed as numeric: %w", field, val, err)
				}
				if fVal < minVal {
					return fmt.Errorf("field '%s' value %f is below minimum limit %f", field, fVal, minVal)
				}
				return nil
			}

		case "max":
			maxVal, err := parseFloat(param)
			if err != nil {
				return nil, fmt.Errorf("invalid parameter '%s' for rule 'max' on field '%s': %w", param, field, err)
			}
			check = func(val interface{}) error {
				if val == nil {
					return nil
				}
				fVal, err := parseFloat(val)
				if err != nil {
					return fmt.Errorf("field '%s' value %v cannot be parsed as numeric: %w", field, val, err)
				}
				if fVal > maxVal {
					return fmt.Errorf("field '%s' value %f is above maximum limit %f", field, fVal, maxVal)
				}
				return nil
			}

		case "type":
			check = func(val interface{}) error {
				if val == nil {
					return nil
				}
				switch param {
				case "int":
					switch val.(type) {
					case int, int64, float64, float32:
						f, _ := parseFloat(val)
						if f == float64(int64(f)) {
							return nil
						}
						return fmt.Errorf("field '%s' is not a valid integer", field)
					case string:
						var i int64
						_, err := fmt.Sscanf(val.(string), "%d", &i)
						if err != nil {
							return fmt.Errorf("field '%s' with value '%v' is not a valid integer", field, val)
						}
						return nil
					default:
						return fmt.Errorf("field '%s' must be an integer", field)
					}
				case "float":
					_, err := parseFloat(val)
					if err != nil {
						return fmt.Errorf("field '%s' is not a float: %w", field, err)
					}
				case "bool":
					switch val.(type) {
					case bool:
						return nil
					case string:
						s := val.(string)
						if s == "true" || s == "false" || s == "1" || s == "0" {
							return nil
						}
						return fmt.Errorf("field '%s' is not a valid boolean string", field)
					default:
						return fmt.Errorf("field '%s' must be a boolean", field)
					}
				case "string":
					if _, ok := val.(string); !ok {
						return fmt.Errorf("field '%s' must be a string", field)
					}
				default:
					return fmt.Errorf("unknown type check parameter: %s", param)
				}
				return nil
			}

		case "regex":
			re, err := regexp.Compile(param)
			if err != nil {
				return nil, fmt.Errorf("invalid regex pattern '%s' for field '%s': %w", param, field, err)
			}
			check = func(val interface{}) error {
				if val == nil {
					return nil
				}
				str, ok := val.(string)
				if !ok {
					str = fmt.Sprintf("%v", val)
				}
				if !re.MatchString(str) {
					return fmt.Errorf("field '%s' with value '%s' does not match pattern '%s'", field, str, param)
				}
				return nil
			}

		default:
			return nil, fmt.Errorf("unknown validation rule: %s", ruleType)
		}

		validators = append(validators, compiledValidator{
			field: field,
			check: check,
		})
	}

	return validators, nil
}

func runValidation(record models.Record, validators []compiledValidator) []string {
	var errs []string
	for _, v := range validators {
		// Fetch field value from ParsedData
		val := record.ParsedData[v.field]
		if err := v.check(val); err != nil {
			errs = append(errs, err.Error())
		}
	}
	return errs
}
