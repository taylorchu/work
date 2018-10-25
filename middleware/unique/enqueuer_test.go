package unique

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
)

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		PoolSize:     10,
		MinIdleConns: 10,
	})
}

func TestEnqueuer(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())

	enq := Enqueuer(&EnqueuerOptions{
		Client: client,
		UniqueFunc: func(*work.Job, *work.EnqueueOptions) ([]byte, time.Duration) {
			return nil, time.Hour
		},
	})

	var called int
	h := enq(func(*work.Job, *work.EnqueueOptions) error {
		called++
		return nil
	})
	for i := 0; i < 3; i++ {
		job := work.NewJob()
		err := h(job, &work.EnqueueOptions{
			Namespace: "n1",
			QueueID:   "q1",
			At:        job.CreatedAt,
		})
		require.NoError(t, err)
	}
	require.Equal(t, 1, called)

	for i := 0; i < 3; i++ {
		require.NoError(t, client.Del("n1:unique:q1:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855").Err())
		job := work.NewJob()
		err := h(job, &work.EnqueueOptions{
			Namespace: "n1",
			QueueID:   "q1",
			At:        job.CreatedAt,
		})
		require.NoError(t, err)
		require.Equal(t, i+2, called)
	}
}

func BenchmarkEnqueuer(b *testing.B) {
	b.StopTimer()

	client := newRedisClient()
	defer client.Close()
	require.NoError(b, client.FlushAll().Err())

	enq := Enqueuer(&EnqueuerOptions{
		Client: client,
		UniqueFunc: func(job *work.Job, _ *work.EnqueueOptions) ([]byte, time.Duration) {
			return []byte(job.ID), time.Hour
		},
	})

	var called int
	h := enq(func(*work.Job, *work.EnqueueOptions) error {
		called++
		return nil
	})

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		job := work.NewJob()
		h(job, &work.EnqueueOptions{
			Namespace: "n1",
			QueueID:   "q1",
			At:        job.CreatedAt,
		})
	}
	b.StopTimer()
	require.Equal(b, b.N, called)
}
