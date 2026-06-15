package utils

// API Route constants
const (
	RouteCreatePipeline = "POST /api/{version}/pipelines"
	RouteListPipelines  = "GET /api/{version}/pipelines"
	RouteGetPipeline   = "GET /api/{version}/pipelines/{id}"
	RouteGetProgress   = "GET /api/{version}/pipelines/{id}/progress"
	RouteGetResults    = "GET /api/{version}/pipelines/{id}/results"
	RouteGetErrors     = "GET /api/{version}/pipelines/{id}/errors"
	RouteCancelPipeline = "PATCH /api/{version}/pipelines/{id}/cancel"
	RouteDeletePipeline = "DELETE /api/{version}/pipelines/{id}"
	RouteMetrics       = "GET /metrics"
)

// API Error and Success Messages
const (
	MsgInvalidPayload       = "Invalid request payload: "
	MsgSourceRequired       = "At least one ingestion source is required"
	MsgSourceFieldsRequired = "Source type and path are required"
	MsgDBRegisterFailed     = "Failed to register job in database: "
	MsgListJobsFailed       = "Failed to list jobs: "
	MsgMissingID            = "Missing path parameter: id"
	MsgJobNotFound          = "Job not found: "
	MsgProgressNotFound     = "Pipeline progress not found: "
	MsgResultsNotReady      = "Job results not generated or not completed yet: "
	MsgRetrieveErrorsFailed = "Failed to retrieve errors: "
	MsgNotRunning           = "Pipeline is not actively running or registry expired"
	MsgDeleteFailed         = "Failed to delete job metadata: "
	MsgCancelDispatched     = "Cancellation request successfully dispatched to running goroutines"
	MsgCancelArchived       = "Cancelled before active run"
	MsgCancelArchivedRes    = "Archived pending job marked cancelled in database"
	MsgDeleteSuccess        = "Job run and database metadata successfully deleted"
	MsgCreateSuccess        = "Pipeline job successfully created and queued"
)
