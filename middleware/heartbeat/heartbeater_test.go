package heartbeat

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redistest"
)

func TestHeartbeater(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-heartbeat}"))

	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace:    "{ns-heartbeat}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60,
	}

	hb := Heartbeater(&HeartbeaterOptions{
		Queue:        work.NewRedisQueue(client),
		InvisibleSec: 30,
		IntervalSec:  1,
	})

	h := hb(func(*work.Job, *work.DequeueOptions) error {
		return nil
	})

	err := h(job, opt)
	require.NoError(t, err)
	require.Equal(t, job.EnqueuedAt.Unix(), job.UpdatedAt.Unix()+30)
	require.NotEqual(t, job.CreatedAt, job.UpdatedAt)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns-heartbeat}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)
}

type countingQueue struct {
	count atomic.Int64
}

func (q *countingQueue) Enqueue(*work.Job, *work.EnqueueOptions) error {
	q.count.Add(1)
	return nil
}

func (q *countingQueue) Dequeue(*work.DequeueOptions) (*work.Job, error) {
	return nil, errors.New("not implemented")
}

func (q *countingQueue) Ack(*work.Job, *work.AckOptions) error {
	return errors.New("not implemented")
}

func TestHeartbeaterStopsAfterPanic(t *testing.T) {
	queue := &countingQueue{}
	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace:    "{ns-heartbeat-panic}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60,
	}

	hb := Heartbeater(&HeartbeaterOptions{
		Queue:        queue,
		InvisibleSec: 30,
		IntervalSec:  1,
	})

	h := hb(func(*work.Job, *work.DequeueOptions) error {
		panic("boom")
	})

	require.Panics(t, func() {
		_ = h(job, opt)
	})
	require.Equal(t, job.EnqueuedAt.Unix(), job.UpdatedAt.Unix()+30)
	require.NotEqual(t, job.CreatedAt, job.UpdatedAt)

	time.Sleep(1100 * time.Millisecond)
	require.EqualValues(t, 1, queue.count.Load())
}
