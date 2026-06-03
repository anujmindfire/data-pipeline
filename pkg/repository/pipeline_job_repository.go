/*
Package repository manages database CRUD transactions using GORM.

PipelineJobRepository manages GORM database queries and updates against the PipelineJob schema.
It handles job inserts, state transitions (e.g. marking jobs as RUNNING or COMPLETED), and progress counts.
*/
package repository

import (
	"data-processing-pipeline/pkg/models"
	"gorm.io/gorm"
	"time"
)

type PipelineJobRepository struct {
	db *gorm.DB
}

func NewPipelineJobRepository(db *gorm.DB) *PipelineJobRepository {
	return &PipelineJobRepository{db: db}
}

func (r *PipelineJobRepository) Save(job *models.PipelineJob) error {
	return r.db.Save(job).Error
}

func (r *PipelineJobRepository) FindOne(id string) (*models.PipelineJob, error) {
	var job models.PipelineJob
	err := r.db.First(&job, "id = ?", id).Error
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *PipelineJobRepository) FindAll() ([]models.PipelineJob, error) {
	var jobs []models.PipelineJob
	err := r.db.Order("created_at desc").Find(&jobs).Error
	return jobs, err
}

func (r *PipelineJobRepository) StartJob(id string) error {
	now := time.Now()
	return r.db.Model(&models.PipelineJob{}).Where("id = ?", id).Updates(map[string]interface{}{
		"status":     "RUNNING",
		"started_at": now,
	}).Error
}

func (r *PipelineJobRepository) SetTotalRecords(id string, total int) error {
	return r.db.Model(&models.PipelineJob{}).Where("id = ?", id).Update("total_records", total).Error
}

func (r *PipelineJobRepository) SetJobCounts(id string, processed, failed int) error {
	return r.db.Model(&models.PipelineJob{}).Where("id = ?", id).Updates(map[string]interface{}{
		"processed_records": processed,
		"failed_records":    failed,
	}).Error
}

func (r *PipelineJobRepository) Complete(id string, status string, errorSummary *string) error {
	now := time.Now()
	updates := map[string]interface{}{
		"status":       status,
		"completed_at": now,
	}
	if errorSummary != nil {
		updates["error_summary"] = *errorSummary
	}
	return r.db.Model(&models.PipelineJob{}).Where("id = ?", id).Updates(updates).Error
}

func (r *PipelineJobRepository) Delete(id string) error {
	return r.db.Delete(&models.PipelineJob{}, "id = ?", id).Error
}
