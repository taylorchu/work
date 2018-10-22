package work

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

// Job is a single unit of work.
type Job struct {
	// ID is the unique id of the job.
	ID string `json:"id"`
	// CreatedAt is set to the time when NewJob is called.
	CreatedAt Time `json:"created_at"`
	// UpdatedAt is when the job is last executed.
	// UpdatedAt is set to the time when NewJob is called initially.
	UpdatedAt Time `json:"updated_at"`

	// payload is raw json bytes.
	Payload json.RawMessage `json:"payload"`

	// If the job previously fails, Retries will be incremented.
	Retries int64 `json:"retries"`
	// If the job previously fails, LastError will be populated with error string.
	LastError string `json:"last_error"`
}

// UnmarshalPayload decodes the payload into a variable.
func (j *Job) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal(j.Payload, v)
}

// MarshalPayload encodes a variable into the payload.
func (j *Job) MarshalPayload(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	j.Payload = b
	return nil
}

// NewJob creates a job.
func NewJob() *Job {
	id := uuid.New().String()
	now := NewTime()
	return &Job{
		ID:        id,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// options validation errors
var (
	ErrEmptyNamespace = errors.New("empty namespace")
	ErrEmptyQueueID   = errors.New("empty queue id")
	ErrAt             = errors.New("at should not be zero")
	ErrInvisibleSec   = errors.New("invisible sec should be > 0")
)

// EnqueueOptions specifies how a job is enqueued.
type EnqueueOptions struct {
	// Namespace is the namespace of the queue.
	Namespace string `json:"ns"`
	// QueueID is the id of the queue.
	QueueID string `json:"queue_id"`
	// At is the current time of the enqueuer.
	// Use this to delay the job execution.
	At Time `json:"at"`
}

// Validate validates EnqueueOptions.
func (opt *EnqueueOptions) Validate() error {
	if opt.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opt.QueueID == "" {
		return ErrEmptyQueueID
	}
	if opt.At.IsZero() {
		return ErrAt
	}
	return nil
}

// Enqueuer enqueues a job.
type Enqueuer interface {
	Enqueue(*Job, *EnqueueOptions) error
}

// DequeueOptions specifies how a job is dequeued.
type DequeueOptions struct {
	// Namespace is the namespace of the queue.
	Namespace string `json:"ns"`
	// QueueID is the id of the queue.
	QueueID string `json:"queue_id"`
	// At is the current time of the dequeuer.
	// Any job that is scheduled before this time can be executed.
	At Time `json:"at"`
	// After the job is dequeued, no other dequeuer can see this job for a while.
	// InvisibleSec controls how long this period is.
	InvisibleSec int64 `json:"invisible_sec"`
}

// Validate validates DequeueOptions.
func (opt *DequeueOptions) Validate() error {
	if opt.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opt.QueueID == "" {
		return ErrEmptyQueueID
	}
	if opt.At.IsZero() {
		return ErrAt
	}
	if opt.InvisibleSec <= 0 {
		return ErrInvisibleSec
	}
	return nil
}

// AckOptions specifies how a job is deleted from a queue.
type AckOptions struct {
	Namespace string `json:"ns"`
	QueueID   string `json:"queue_id"`
}

// Validate validates AckOptions.
func (opt *AckOptions) Validate() error {
	if opt.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opt.QueueID == "" {
		return ErrEmptyQueueID
	}
	return nil
}

// Dequeuer dequeues a job.
// If the job is processed successfully, call Ack() to delete the job from the queue.
type Dequeuer interface {
	Dequeue(*DequeueOptions) (*Job, error)
	Ack(*Job, *AckOptions) error
}

// Queue can enqueue and dequeue jobs.
type Queue interface {
	Enqueuer
	Dequeuer
}
