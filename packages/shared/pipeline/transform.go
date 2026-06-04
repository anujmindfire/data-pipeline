package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"data-processing-pipeline/packages/shared/models"
)

// StartTransformationStage spawns worker goroutines to transform validated records in parallel.
func StartTransformationStage(ctx context.Context, jobSpec *models.JobSpec, validatedCh <-chan models.Record, transformedCh chan<- models.Record, errorCh chan<- ErrorEvent, progressCh chan<- ProgressEvent) <-chan struct{} {
	done := make(chan struct{})
	var wg sync.WaitGroup
	numWorkers := jobSpec.WorkerPoolSizes.Transformation
	if numWorkers <= 0 {
		numWorkers = 1
	}

	// Pre-compile transformation rules for fast performance
	compiledTransformers, compileErr := compileTransformRules(jobSpec.TransformRules)
	if compileErr != nil {
		errorCh <- ErrorEvent{
			JobID:        jobSpec.ID,
			Stage:        "transform",
			ErrorMessage: fmt.Sprintf("failed to pre-compile transformation rules: %v", compileErr),
		}
		close(transformedCh)
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
				case record, ok := <-validatedCh:
					if !ok {
						return
					}

					// Process transformation
					if err := runTransformations(&record, compiledTransformers); err != nil {
						// Transformation failed (e.g. invalid type cast after check, although rare)
						errorCh <- ErrorEvent{
							JobID:        record.JobID,
							Stage:        "transform",
							RawData:      string(record.RawPayload),
							ErrorMessage: err.Error(),
						}
						// Even if transform fails, do we filter it?
						// The prompt says "Filter or mark invalid records while continuing processing."
						// So we mark it invalid and do not forward to aggregation.
						record.IsValid = false
						continue
					}

					// Send to aggregation stage
					select {
					case <-ctx.Done():
						return
					case transformedCh <- record:
					}
				}
			}
		}(i)
	}

	// Fan-in: Wait for workers to complete and close output channel
	go func() {
		wg.Wait()
		close(transformedCh)
		progressCh <- ProgressEvent{
			JobID:    jobSpec.ID,
			Stage:    "transformation",
			Type:     ProgressStageCompleted,
			Delta:    0,
		}
		close(done)
	}()
	return done
}

type compiledTransformer struct {
	field string
	apply func(payload map[string]any) error
}

func compileTransformRules(rules []models.TransformRuleSpec) ([]compiledTransformer, error) {
	transformers := make([]compiledTransformer, 0, len(rules))

	for _, spec := range rules {
		ruleType := spec.Rule
		field := spec.Field
		param := spec.Param

		var apply func(payload map[string]any) error

		switch ruleType {
		case "cast":
			switch param {
			case "float":
				apply = func(payload map[string]any) error {
					if val, ok := payload[field]; ok && val != nil {
						f, err := parseFloat(val)
						if err != nil {
							return fmt.Errorf("transform cast failed for field '%s' with value '%v' to float: %w", field, val, err)
						}
						payload[field] = f
					}
					return nil
				}
			case "int":
				apply = func(payload map[string]any) error {
					if val, ok := payload[field]; ok && val != nil {
						f, err := parseFloat(val)
						if err != nil {
							return fmt.Errorf("transform cast failed for field '%s' with value '%v' to int: %w", field, val, err)
						}
						payload[field] = int64(f)
					}
					return nil
				}
			case "string":
				apply = func(payload map[string]any) error {
					if val, ok := payload[field]; ok {
						if val == nil {
							payload[field] = ""
						} else {
							payload[field] = fmt.Sprintf("%v", val)
						}
					}
					return nil
				}
			case "bool":
				apply = func(payload map[string]any) error {
					if val, ok := payload[field]; ok && val != nil {
						switch v := val.(type) {
						case bool:
							payload[field] = v
						case string:
							b, err := strconv.ParseBool(strings.TrimSpace(strings.ToLower(v)))
							if err != nil {
								return fmt.Errorf("transform cast failed for field '%s' with value '%v' to bool: %w", field, val, err)
							}
							payload[field] = b
						case float64, float32, int, int64:
							f, _ := parseFloat(val)
							payload[field] = f != 0
						default:
							payload[field] = false
						}
					}
					return nil
				}
			default:
				return nil, fmt.Errorf("unknown cast type parameter: %s", param)
			}

		case "trim":
			apply = func(payload map[string]any) error {
				if val, ok := payload[field]; ok && val != nil {
					if str, ok := val.(string); ok {
						payload[field] = strings.TrimSpace(str)
					}
				}
				return nil
			}

		case "lower":
			apply = func(payload map[string]any) error {
				if val, ok := payload[field]; ok && val != nil {
					if str, ok := val.(string); ok {
						payload[field] = strings.ToLower(str)
					}
				}
				return nil
			}

		case "upper":
			apply = func(payload map[string]any) error {
				if val, ok := payload[field]; ok && val != nil {
					if str, ok := val.(string); ok {
						payload[field] = strings.ToUpper(str)
					}
				}
				return nil
			}

		case "enrich_time":
			apply = func(payload map[string]any) error {
				// Adds current ISO 8601 UTC time
				payload[field] = time.Now().UTC().Format(time.RFC3339)
				return nil
			}

		case "add_constant":
			constVal, err := parseFloat(param)
			if err != nil {
				return nil, fmt.Errorf("invalid constant value '%s' for rule 'add_constant' on field '%s': %w", param, field, err)
			}
			apply = func(payload map[string]any) error {
				if val, ok := payload[field]; ok && val != nil {
					f, err := parseFloat(val)
					if err != nil {
						return fmt.Errorf("transform add_constant failed for field '%s' with non-numeric value '%v': %w", field, val, err)
					}
					payload[field] = f + constVal
				}
				return nil
			}

		default:
			return nil, fmt.Errorf("unknown transformation rule: %s", ruleType)
		}

		transformers = append(transformers, compiledTransformer{
			field: field,
			apply: apply,
		})
	}

	return transformers, nil
}

func runTransformations(record *models.Record, transformers []compiledTransformer) error {
	for _, t := range transformers {
		if err := t.apply(record.ParsedData); err != nil {
			return err
		}
	}
	return nil
}
