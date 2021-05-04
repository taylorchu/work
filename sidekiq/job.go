package sidekiq

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/taylorchu/work"
)

// See https://github.com/mperham/sidekiq/wiki/Job-Format.
type sidekiqJob struct {
	Class        string          `json:"class"`
	ID           string          `json:"jid"`
	Args         json.RawMessage `json:"args"`
	CreatedAt    float64         `json:"created_at"`
	EnqueuedAt   float64         `json:"enqueued_at,omitempty"`
	Queue        string          `json:"queue,omitempty"`
	Retry        json.RawMessage `json:"retry,omitempty"`
	RetryCount   int64           `json:"retry_count,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	ErrorClass   string          `json:"error_class,omitempty"`
	FailedAt     float64         `json:"failed_at,omitempty"`
	RetriedAt    float64         `json:"retried_at,omitempty"`
}

// sidekiq job validation errors
var (
	ErrJobEmptyClass = errors.New("sidekiq: empty job class")
	ErrJobEmptyID    = errors.New("sidekiq: empty job id")
	ErrJobCreatedAt  = errors.New("sidekiq: job created_at should be > 0")
	ErrJobEnqueuedAt = errors.New("sidekiq: job enqueued_at should be > 0")
	ErrJobArgs       = errors.New("sidekiq: job args should be an array")
)

func (j *sidekiqJob) Validate() error {
	if j.Class == "" {
		return ErrJobEmptyClass
	}
	if j.ID == "" {
		return ErrJobEmptyID
	}
	if j.CreatedAt <= 0 {
		return ErrJobCreatedAt
	}
	if j.EnqueuedAt <= 0 {
		return ErrJobEnqueuedAt
	}
	if !(bytes.HasPrefix(j.Args, []byte("[")) && bytes.HasSuffix(j.Args, []byte("]"))) {
		return ErrJobArgs
	}
	return nil
}

func newSidekiqJob(job *work.Job, sqQueue, sqClass string) (*sidekiqJob, error) {
	errorClass := ""
	failedAt := int64(0)
	retriedAt := int64(0)
	if job.LastError != "" {
		errorClass = "StandardError"
		failedAt = job.UpdatedAt.Unix()
		retriedAt = job.UpdatedAt.Unix()
	}
	sqJob := sidekiqJob{
		Class:        sqClass,
		ID:           job.ID,
		Args:         job.Payload,
		CreatedAt:    float64(job.CreatedAt.Unix()),
		EnqueuedAt:   float64(job.EnqueuedAt.Unix()),
		Queue:        sqQueue,
		Retry:        []byte("true"),
		RetryCount:   job.Retries,
		ErrorMessage: job.LastError,
		ErrorClass:   errorClass,
		FailedAt:     float64(failedAt),
		RetriedAt:    float64(retriedAt),
	}
	return &sqJob, nil
}

func newJob(sqJob *sidekiqJob) (*work.Job, error) {
	updatedAt := sqJob.CreatedAt
	for _, ts := range []float64{sqJob.FailedAt, sqJob.RetriedAt} {
		if ts > updatedAt {
			updatedAt = ts
		}
	}
	job := work.Job{
		ID:         sqJob.ID,
		Payload:    sqJob.Args,
		CreatedAt:  time.Unix(int64(sqJob.CreatedAt), 0),
		UpdatedAt:  time.Unix(int64(updatedAt), 0),
		EnqueuedAt: time.Unix(int64(sqJob.EnqueuedAt), 0),
		Retries:    sqJob.RetryCount,
		LastError:  sqJob.ErrorMessage,
	}
	return &job, nil
}
