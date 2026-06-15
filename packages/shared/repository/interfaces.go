package repository

import "data-processing-pipeline/packages/shared/models"

type IPipelineJobRepository interface {
	Save(job *models.PipelineJob) error
	FindOne(id string) (*models.PipelineJob, error)
	FindAll() ([]models.PipelineJob, error)
	StartJob(id string) error
	SetTotalRecords(id string, total int) error
	SetJobCounts(id string, processed, failed int) error
	Complete(id string, status string, errorSummary *string) error
	Delete(id string) error
	FindPendingJob() (*models.PipelineJob, error)
}

type IJobErrorRepository interface {
	Save(jobError *models.JobError) error
	FindByJobID(jobID string) ([]models.JobError, error)
}

type IJobResultsRepository interface {
	Save(results *models.JobResults) error
	FindOne(jobID string) (*models.JobResults, error)
}
