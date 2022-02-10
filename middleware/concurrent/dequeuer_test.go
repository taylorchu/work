package concurrent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redistest"
)

func TestDequeuer(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	opt := &work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60,
	}
	var called int
	h1 := func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return work.NewJob(), nil
	}
	h2 := func(*work.Job, *work.DequeueOptions) error {
		return nil
	}
	runJob := func(m1 work.DequeueMiddleware, m2 work.HandleMiddleware, opt *work.DequeueOptions) error {
		job, err := m1(h1)(opt)
		if err != nil {
			return err
		}
		err = m2(h2)(job, opt)
		if err != nil {
			return err
		}
		return nil
	}

	for i := 0; i < 3; i++ {
		m1, m2 := Dequeuer(&DequeuerOptions{
			Client:   client,
			Max:      2,
			workerID: fmt.Sprintf("w%d", i),
		})
		err := runJob(m1, m2, opt)
		require.NoError(t, err)
	}
	require.Equal(t, 3, called)

	// worker 0, 1 get the lock
	// worker 2 is locked
	for i := 0; i < 3; i++ {
		m1, m2 := Dequeuer(&DequeuerOptions{
			Client:        client,
			Max:           2,
			workerID:      fmt.Sprintf("w%d", i),
			disableUnlock: true,
		})
		err := runJob(m1, m2, opt)
		if i <= 1 {
			require.NoError(t, err)
		} else {
			require.Equal(t, work.ErrEmptyQueue, err)
		}
	}
	require.Equal(t, 5, called)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:lock:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 2)
	require.Equal(t, "w0", z[0].Member)
	require.Equal(t, "w1", z[1].Member)
	require.EqualValues(t, opt.At.Unix()+60, z[0].Score)
	require.EqualValues(t, opt.At.Unix()+60, z[1].Score)

	require.NoError(t, client.ZRem(context.Background(), "{ns1}:lock:q1", "w1").Err())
	optLater := *opt
	optLater.At = opt.At.Add(10 * time.Second)
	// worker 0 is locked already
	for i := 0; i < 3; i++ {
		m1, m2 := Dequeuer(&DequeuerOptions{
			Client:        client,
			Max:           2,
			workerID:      "w0",
			disableUnlock: true,
		})
		err := runJob(m1, m2, &optLater)
		require.Equal(t, work.ErrEmptyQueue, err)
	}
	require.Equal(t, 5, called)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:lock:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "w0", z[0].Member)
	require.EqualValues(t, opt.At.Unix()+60, z[0].Score)

	// lock key expired
	// worker 3, 4 get lock
	optExpired := *opt
	optExpired.At = opt.At.Add(60 * time.Second)
	for i := 3; i < 6; i++ {
		m1, m2 := Dequeuer(&DequeuerOptions{
			Client:        client,
			Max:           2,
			workerID:      fmt.Sprintf("w%d", i),
			disableUnlock: true,
		})
		err := runJob(m1, m2, &optExpired)
		if i < 5 {
			require.NoError(t, err)
		} else {
			require.Equal(t, work.ErrEmptyQueue, err)
		}
	}
	require.Equal(t, 7, called)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:lock:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 2)
	require.Equal(t, "w3", z[0].Member)
	require.Equal(t, "w4", z[1].Member)
	require.EqualValues(t, optExpired.At.Unix()+60, z[0].Score)
	require.EqualValues(t, optExpired.At.Unix()+60, z[1].Score)
}

func BenchmarkConcurrency(b *testing.B) {
	b.StopTimer()

	client := redistest.NewClient()
	defer client.Close()
	require.NoError(b, redistest.Reset(client))

	opt := &work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60,
	}
	var called int
	h1 := func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return work.NewJob(), nil
	}
	h2 := func(*work.Job, *work.DequeueOptions) error {
		return nil
	}
	runJob := func(m1 work.DequeueMiddleware, m2 work.HandleMiddleware, opt *work.DequeueOptions) error {
		job, err := m1(h1)(opt)
		if err != nil {
			return err
		}
		err = m2(h2)(job, opt)
		if err != nil {
			return err
		}
		return nil
	}

	m1, m2 := Dequeuer(&DequeuerOptions{
		Client: client,
		Max:    1,
	})
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		runJob(m1, m2, opt)
	}
	b.StopTimer()
	require.Equal(b, b.N, called)
}
