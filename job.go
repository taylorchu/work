package work

import (
	"encoding/json"
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

type EnqueueOptions struct {
	Prefix string `json:"prefix"`
	At     Time   `json:"at"`
}

type Enqueuer interface {
	Enqueue(*Job, *EnqueueOptions) error
}

type DequeueOptions struct {
	Prefix    string `json:"prefix"`
	At        Time   `json:"at"`
	LockedBy  string `json:"locked_by"`
	LockedAt  Time   `json:"locked_at"`
	LockedSec int64  `json:"locked_sec"`
}

type Dequeuer interface {
	Dequeue(*DequeueOptions) (*Job, error)
	Ack(*Job, *DequeueOptions) error
}

type Queue interface {
	Enqueuer
	Dequeuer
}
