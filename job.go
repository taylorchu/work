package work

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Time struct {
	time.Time
}

func (t *Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Unix())
}

func (t *Time) UnmarshalJSON(b []byte) error {
	var ts int64
	err := json.Unmarshal(b, &ts)
	if err != nil {
		return err
	}
	t.Time = time.Unix(ts, 0)
	return nil
}

type Job struct {
	ID        string          `json:"id"`
	CreatedAt Time            `json:"created_at"`
	UpdatedAt Time            `json:"updated_at"`
	Payload   json.RawMessage `json:"payload"`

	Retries   int64  `json:"retries"`
	LastError string `json:"last_error"`
}

func (j *Job) UnmarshalPayload(v interface{}) error {
	return json.Unmarshal(j.Payload, v)
}

func (j *Job) MarshalPayload(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	j.Payload = b
	return nil
}

func NewJob() *Job {
	id := uuid.New().String()
	now := Time{time.Now().Truncate(time.Second)}
	return &Job{
		ID:        id,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

var (
	ErrEmptyNamespace = errors.New("empty namespace")
	ErrEmptyQueueID   = errors.New("empty queue id")
	ErrAt             = errors.New("at should not be zero")
	ErrLockedAt       = errors.New("locked at should be > 0")
)

type EnqueueOptions struct {
	Namespace string `json:"ns"`
	QueueID   string `json:"queue_id"`
	At        Time   `json:"at"`
}

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

type Enqueuer interface {
	Enqueue(*Job, *EnqueueOptions) error
}

type DequeueOptions struct {
	Namespace string `json:"ns"`
	QueueID   string `json:"queue_id"`
	At        Time   `json:"at"`
	LockedSec int64  `json:"locked_sec"`
}

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
	if opt.LockedSec <= 0 {
		return ErrLockedAt
	}
	return nil
}

type AckOptions struct {
	Namespace string `json:"ns"`
	QueueID   string `json:"queue_id"`
}

func (opt *AckOptions) Validate() error {
	if opt.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opt.QueueID == "" {
		return ErrEmptyQueueID
	}
	return nil
}

type Dequeuer interface {
	Dequeue(*DequeueOptions) (*Job, error)
	Ack(*Job, *AckOptions) error
}

type Queue interface {
	Enqueuer
	Dequeuer
}
