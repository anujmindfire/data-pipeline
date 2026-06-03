/*
Package repository manages database CRUD transactions using GORM.

JobResultsRepository manages GORM persistent operations for finalized pipeline results and summaries.
*/
package repository

import (
	"data-processing-pipeline/pkg/models"
	"gorm.io/gorm"
)

type JobResultsRepository struct {
	db *gorm.DB
}

func NewJobResultsRepository(db *gorm.DB) *JobResultsRepository {
	return &JobResultsRepository{db: db}
}

func (r *JobResultsRepository) Save(results *models.JobResults) error {
	return r.db.Save(results).Error
}

func (r *JobResultsRepository) FindOne(jobID string) (*models.JobResults, error) {
	var results models.JobResults
	err := r.db.First(&results, "job_id = ?", jobID).Error
	if err != nil {
		return nil, err
	}
	return &results, nil
}
