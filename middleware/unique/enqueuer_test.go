package unique

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redistest"
)

func TestEnqueuerBypass(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	enq := Enqueuer(&EnqueuerOptions{
		Client: client,
		UniqueFunc: func(*work.Job, *work.EnqueueOptions) ([]byte, time.Duration, error) {
			return nil, time.Hour, nil
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
			Namespace: "{ns1}",
			QueueID:   "q1",
		})
		require.NoError(t, err)
	}
	require.Equal(t, 3, called)
}

func TestEnqueuer(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	enq := Enqueuer(&EnqueuerOptions{
		Client: client,
		UniqueFunc: func(*work.Job, *work.EnqueueOptions) ([]byte, time.Duration, error) {
			return []byte("test"), time.Hour, nil
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
			Namespace: "{ns1}",
			QueueID:   "q1",
		})
		require.NoError(t, err)
	}
	require.Equal(t, 1, called)

	for i := 0; i < 3; i++ {
		require.NoError(t, client.Del(context.Background(), "{ns1}:unique:q1:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08").Err())
		job := work.NewJob()
		err := h(job, &work.EnqueueOptions{
			Namespace: "{ns1}",
			QueueID:   "q1",
		})
		require.NoError(t, err)
		require.Equal(t, i+2, called)
	}
}

func BenchmarkEnqueuer(b *testing.B) {
	b.StopTimer()

	client := redistest.NewClient()
	defer client.Close()
	require.NoError(b, redistest.Reset(client))

	enq := Enqueuer(&EnqueuerOptions{
		Client: client,
		UniqueFunc: func(job *work.Job, _ *work.EnqueueOptions) ([]byte, time.Duration, error) {
			return []byte(job.ID), time.Hour, nil
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
			Namespace: "{ns1}",
			QueueID:   "q1",
		})
	}
	b.StopTimer()
	require.Equal(b, b.N, called)
}
