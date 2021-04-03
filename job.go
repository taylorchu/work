package work

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Job is a single unit of work.
type Job struct {
	// ID is the unique id of a job.
	ID string `msgpack:"id"`
	// CreatedAt is set to the time when NewJob() is called.
	CreatedAt time.Time `msgpack:"created_at"`
	// UpdatedAt is when the job is last executed.
	// UpdatedAt is set to the time when NewJob() is called initially.
	UpdatedAt time.Time `msgpack:"updated_at"`
	// EnqueuedAt is when the job will be executed next.
	// EnqueuedAt is set to the time when NewJob() is called initially.
	EnqueuedAt time.Time `msgpack:"enqueued_at"`

	// Payload is raw bytes.
	Payload []byte `msgpack:"payload"`

	// If the job previously fails, Retries will be incremented.
	Retries int64 `msgpack:"retries"`
	// If the job previously fails, LastError will be populated with error string.
	LastError string `msgpack:"last_error"`
}

// InvalidJobPayloadError wraps json or msgpack decoding error.
type InvalidJobPayloadError struct {
	Err error
}

func (e *InvalidJobPayloadError) Error() string {
	return fmt.Sprintf("work: invalid job payload: %v", e.Err)
}

// UnmarshalPayload decodes the msgpack payload into a variable.
func (j *Job) UnmarshalPayload(v interface{}) error {
	// used in middleware/discard package.
	err := unmarshal(bytes.NewReader(j.Payload), v)
	if err != nil {
		return &InvalidJobPayloadError{Err: err}
	}
	return nil
}

// UnmarshalJSONPayload decodes the JSON payload into a variable.
func (j *Job) UnmarshalJSONPayload(v interface{}) error {
	// used in middleware/discard package.
	err := json.Unmarshal(j.Payload, v)
	if err != nil {
		return &InvalidJobPayloadError{Err: err}
	}
	return nil
}

// MarshalPayload encodes a variable into the msgpack payload.
func (j *Job) MarshalPayload(v interface{}) error {
	b, err := marshal(v)
	if err != nil {
		return err
	}
	j.Payload = b
	return nil
}

// MarshalJSONPayload encodes a variable into the JSON payload.
func (j *Job) MarshalJSONPayload(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	j.Payload = b
	return nil
}

// NewJob creates a job.
func NewJob() *Job {
	id := uuid.Must(uuid.NewUUID()).String()
	now := time.Now().Truncate(time.Second)
	return &Job{
		ID:         id,
		CreatedAt:  now,
		UpdatedAt:  now,
		EnqueuedAt: now,
	}
}

// Delay creates a job that can be executed in future.
func (j Job) Delay(d time.Duration) *Job {
	j.EnqueuedAt = j.EnqueuedAt.Add(d)
	return &j
}

// WithPayload adds payload to the job.
func (j Job) WithPayload(v interface{}) (*Job, error) {
	err := j.MarshalPayload(v)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// options validation errors
var (
	ErrEmptyNamespace = errors.New("work: empty namespace")
	ErrEmptyQueueID   = errors.New("work: empty queue id")
	ErrAt             = errors.New("work: at should not be zero")
	ErrInvisibleSec   = errors.New("work: invisible sec should be >= 0")
)

// EnqueueOptions specifies how a job is enqueued.
type EnqueueOptions struct {
	// Namespace is the namespace of a queue.
	Namespace string
	// QueueID is the id of a queue.
	QueueID string
}

// Validate validates EnqueueOptions.
func (opt *EnqueueOptions) Validate() error {
	if opt.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opt.QueueID == "" {
		return ErrEmptyQueueID
	}
	return nil
}

// Enqueuer enqueues a job.
type Enqueuer interface {
	Enqueue(*Job, *EnqueueOptions) error
}

// ExternalEnqueuer enqueues a job with other queue protocol.
// Queue adaptor that implements this can publish jobs directly to other types of queue systems.
type ExternalEnqueuer interface {
	ExternalEnqueue(*Job, *EnqueueOptions) error
}

// DequeueOptions specifies how a job is dequeued.
type DequeueOptions struct {
	// Namespace is the namespace of a queue.
	Namespace string
	// QueueID is the id of a queue.
	QueueID string
	// At is the current time of the dequeuer.
	// Any job that is scheduled before this can be executed.
	At time.Time
	// After the job is dequeued, no other dequeuer can see this job for a while.
	// InvisibleSec controls how long this period is.
	InvisibleSec int64
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
	if opt.InvisibleSec < 0 {
		return ErrInvisibleSec
	}
	return nil
}

// AckOptions specifies how a job is deleted from a queue.
type AckOptions struct {
	Namespace string
	QueueID   string
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

var (
	// ErrEmptyQueue is returned if Dequeue() is called on an empty queue.
	ErrEmptyQueue = errors.New("work: no job is found")
)

// Dequeuer dequeues a job.
// If a job is processed successfully, call Ack() to delete the job.
type Dequeuer interface {
	Dequeue(*DequeueOptions) (*Job, error)
	Ack(*Job, *AckOptions) error
}

// Queue can enqueue and dequeue jobs.
type Queue interface {
	Enqueuer
	Dequeuer
}

// BulkEnqueuer enqueues jobs in a batch.
type BulkEnqueuer interface {
	BulkEnqueue([]*Job, *EnqueueOptions) error
}

// ExternalBulkEnqueuer enqueues jobs in a batch with other queue protocol.
// Queue adaptor that implements this can publish jobs directly to other types of queue systems.
type ExternalBulkEnqueuer interface {
	ExternalBulkEnqueue([]*Job, *EnqueueOptions) error
}

// BulkDequeuer dequeues jobs in a batch.
type BulkDequeuer interface {
	BulkDequeue(int64, *DequeueOptions) ([]*Job, error)
	BulkAck([]*Job, *AckOptions) error
}

// FindOptions specifies how a job is searched from a queue.
type FindOptions struct {
	Namespace string
}

// Validate validates FindOptions.
func (opt *FindOptions) Validate() error {
	if opt.Namespace == "" {
		return ErrEmptyNamespace
	}
	return nil
}

// BulkJobFinder finds jobs by ids.
// It allows third-party tools to get job status, or modify job by re-enqueue.
// It returns nil if the job is no longer in the queue.
// The length of the returned job list will be equal to the length of jobIDs.
type BulkJobFinder interface {
	BulkFind(jobIDs []string, opts *FindOptions) ([]*Job, error)
}
