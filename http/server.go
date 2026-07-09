package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/taylorchu/work"
)

// ServerOptions specifies how http server can manage work queues.
// It implements the generated StrictServerInterface.
type ServerOptions struct {
	Queue work.Queue
}

func (opts *ServerOptions) DeleteJob(ctx context.Context, request DeleteJobRequestObject) (DeleteJobResponseObject, error) {
	queue, ok := opts.Queue.(interface {
		work.Queue
		work.BulkJobFinder
	})
	if !ok {
		return DeleteJob404Response{}, nil
	}
	namespace := request.Params.Namespace
	queueID := request.Params.QueueID
	jobID := request.Params.JobID

	jobs, err := queue.BulkFind([]string{jobID}, &work.FindOptions{
		Namespace: namespace,
	})
	if err != nil {
		return DeleteJob500JSONResponse{ErrorJSONResponse{Error: err.Error()}}, nil
	}
	job := &work.Job{ID: jobID}
	if len(jobs) == 1 && jobs[0] != nil {
		if err := queue.Ack(jobs[0], &work.AckOptions{
			Namespace: namespace,
			QueueID:   queueID,
		}); err != nil {
			return DeleteJob500JSONResponse{ErrorJSONResponse{Error: err.Error()}}, nil
		}
		job = jobs[0]
	}
	return DeleteJob200JSONResponse{
		Namespace: namespace,
		QueueID:   queueID,
		Job:       job,
	}, nil
}

func (opts *ServerOptions) GetJob(ctx context.Context, request GetJobRequestObject) (GetJobResponseObject, error) {
	queue, ok := opts.Queue.(interface {
		work.Queue
		work.BulkJobFinder
	})
	if !ok {
		return GetJob404Response{}, nil
	}
	namespace := request.Params.Namespace
	jobID := request.Params.JobID

	jobs, err := queue.BulkFind([]string{jobID}, &work.FindOptions{
		Namespace: namespace,
	})
	if err != nil {
		return GetJob500JSONResponse{ErrorJSONResponse{Error: err.Error()}}, nil
	}
	job := &work.Job{ID: jobID}
	if len(jobs) == 1 && jobs[0] != nil {
		job = jobs[0]
	}
	return GetJob200JSONResponse{
		Namespace: namespace,
		Status:    JobStatus(jobStatus(job)),
		Job:       job,
	}, nil
}

func (opts *ServerOptions) CreateJob(ctx context.Context, request CreateJobRequestObject) (CreateJobResponseObject, error) {
	copt := request.Body
	if copt.ID != "" {
		if finder, ok := opts.Queue.(work.BulkJobFinder); ok {
			// best effort to check for duplicates
			jobs, err := finder.BulkFind([]string{copt.ID}, &work.FindOptions{
				Namespace: copt.Namespace,
			})
			if err != nil {
				return CreateJob500JSONResponse{ErrorJSONResponse{Error: err.Error()}}, nil
			}
			if len(jobs) == 1 && jobs[0] != nil {
				return CreateJob200JSONResponse{
					Namespace: copt.Namespace,
					QueueID:   copt.QueueID,
					Job:       jobs[0],
				}, nil
			}
		}
	}
	job := work.NewJob().Delay(time.Duration(copt.Delay))
	if copt.ID != "" {
		job.ID = copt.ID
	}
	job.Payload = copt.Payload
	if err := opts.Queue.Enqueue(job, &work.EnqueueOptions{
		Namespace: copt.Namespace,
		QueueID:   copt.QueueID,
	}); err != nil {
		return CreateJob500JSONResponse{ErrorJSONResponse{Error: err.Error()}}, nil
	}
	return CreateJob200JSONResponse{
		Namespace: copt.Namespace,
		QueueID:   copt.QueueID,
		Job:       job,
	}, nil
}

func (opts *ServerOptions) GetMetrics(ctx context.Context, request GetMetricsRequestObject) (GetMetricsResponseObject, error) {
	queue, ok := opts.Queue.(interface {
		work.Queue
		work.MetricsExporter
	})
	if !ok {
		return GetMetrics404Response{}, nil
	}
	namespace := request.Params.Namespace
	queueID := request.Params.QueueID

	metrics, err := queue.GetQueueMetrics(&work.QueueMetricsOptions{
		Namespace: namespace,
		QueueID:   queueID,
		At:        time.Now(),
	})
	if err != nil {
		return GetMetrics500JSONResponse{ErrorJSONResponse{Error: err.Error()}}, nil
	}
	return GetMetrics200JSONResponse{
		Namespace:      metrics.Namespace,
		QueueID:        metrics.QueueID,
		ReadyTotal:     metrics.ReadyTotal,
		ScheduledTotal: metrics.ScheduledTotal,
		Total:          metrics.ReadyTotal + metrics.ScheduledTotal,
		Latency:        metrics.Latency,
	}, nil
}

// jsonError returns an error handler that writes err as a JSON Error body with
// the given status, so every error path shares the {"error": ...} shape.
func jsonError(status int) func(http.ResponseWriter, *http.Request, error) {
	return func(rw http.ResponseWriter, r *http.Request, err error) {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(status)
		json.NewEncoder(rw).Encode(Error{Error: err.Error()})
	}
}

// NewServer creates new http server that manages work queues.
func NewServer(opts *ServerOptions) http.Handler {
	strict := NewStrictHandlerWithOptions(opts, nil, StrictHTTPServerOptions{
		// Malformed request body -> 400; response encoding failure -> 500.
		RequestErrorHandlerFunc:  jsonError(http.StatusBadRequest),
		ResponseErrorHandlerFunc: jsonError(http.StatusInternalServerError),
	})
	// Missing/invalid required query parameter -> 400.
	return HandlerWithOptions(strict, StdHTTPServerOptions{
		ErrorHandlerFunc: jsonError(http.StatusBadRequest),
	})
}

func jobStatus(job *work.Job) string {
	if job.EnqueuedAt.IsZero() {
		return "completed"
	}
	if job.EnqueuedAt.After(time.Now()) {
		return "scheduled"
	}
	return "ready"
}
