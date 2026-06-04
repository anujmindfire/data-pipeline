/*
Package repository manages database CRUD transactions using GORM.

JobErrorRepository handles inserting record processing errors and retrieving failure logs
by Job ID for monitoring audit trails.
*/
package repository

import (
	"data-processing-pipeline/packages/shared/models"
	"gorm.io/gorm"
)

type JobErrorRepository struct {
	db *gorm.DB
}

func NewJobErrorRepository(db *gorm.DB) *JobErrorRepository {
	return &JobErrorRepository{db: db}
}

func (r *JobErrorRepository) Save(jobError *models.JobError) error {
	return r.db.Create(jobError).Error
}

func (r *JobErrorRepository) FindByJobID(jobID string) ([]models.JobError, error) {
	var errs []models.JobError
	err := r.db.Order("occurred_at asc").Find(&errs, "job_id = ?", jobID).Error
	return errs, err
}
