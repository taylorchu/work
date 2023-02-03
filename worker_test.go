package work

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work/redistest"
)

func TestWorkerStartStop(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	w := NewWorker(&WorkerOptions{
		Namespace: "{ns1}",
		Queue:     NewRedisQueue(client),
	})
	err := w.Register("test",
		func(*Job, *DequeueOptions) error { return nil },
		&JobOptions{
			MaxExecutionTime: time.Second,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		w.Start()
		w.Stop()
	}
}

func TestWorkerExportMetrics(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	w := NewWorker(&WorkerOptions{
		Namespace: "{ns1}",
		Queue:     NewRedisQueue(client),
	})
	err := w.Register("test",
		func(*Job, *DequeueOptions) error { return nil },
		&JobOptions{
			MaxExecutionTime: time.Second,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)

	all, err := w.ExportMetrics()
	require.NoError(t, err)
	require.Len(t, all.Queue, 1)
	require.Equal(t, all.Queue[0].Namespace, "{ns1}")
	require.Equal(t, all.Queue[0].QueueID, "test")
}

func waitEmpty(client redis.UniversalClient, key string, timeout time.Duration) error {
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	const tickIntv = 10 * time.Millisecond
	ticker := time.NewTicker(tickIntv)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutTimer.C:
			return errors.New("timeout")
		case <-ticker.C:
			z, err := client.ZRangeByScoreWithScores(
				context.Background(),
				key,
				&redis.ZRangeBy{
					Min: "-inf",
					Max: fmt.Sprint(time.Now().Unix()),
				}).Result()
			if err != nil {
				return err
			}
			if len(z) == 0 {
				time.Sleep(tickIntv)
				return nil
			}
		}
	}
}

func TestWorkerRunJobMultiQueue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	type message struct {
		Text string
	}

	w := NewWorker(&WorkerOptions{
		Namespace: "{ns1}",
		Queue:     NewRedisQueue(client),
	})
	err := w.Register("test1",
		func(job *Job, _ *DequeueOptions) error {
			var msg message
			job.UnmarshalPayload(&msg)
			if msg.Text != "test1" {
				return errors.New("bad payload")
			}
			return nil
		},
		&JobOptions{
			MaxExecutionTime: time.Minute,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)
	err = w.Register("test2",
		func(job *Job, _ *DequeueOptions) error {
			var msg message
			job.UnmarshalPayload(&msg)
			if msg.Text != "test2" {
				return errors.New("bad payload")
			}
			return nil
		},
		&JobOptions{
			MaxExecutionTime: time.Minute,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		job := NewJob()
		err := job.MarshalPayload(message{Text: "test1"})
		require.NoError(t, err)

		err = w.opt.Queue.Enqueue(job, &EnqueueOptions{
			Namespace: "{ns1}",
			QueueID:   "test1",
		})
		require.NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		job := NewJob()
		err := job.MarshalPayload(message{Text: "test2"})
		require.NoError(t, err)

		err = w.opt.Queue.Enqueue(job, &EnqueueOptions{
			Namespace: "{ns1}",
			QueueID:   "test2",
		})
		require.NoError(t, err)
	}

	count, err := client.ZCard(context.Background(), "{ns1}:queue:test1").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	count, err = client.ZCard(context.Background(), "{ns1}:queue:test2").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	w.Start()
	err = waitEmpty(client, "{ns1}:queue:test1", 10*time.Second)
	require.NoError(t, err)
	err = waitEmpty(client, "{ns1}:queue:test2", 10*time.Second)
	require.NoError(t, err)
	w.Stop()

	count, err = client.ZCard(context.Background(), "{ns1}:queue:test1").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	count, err = client.ZCard(context.Background(), "{ns1}:queue:test2").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, count)
}

func TestWorkerRunJob(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	w := NewWorker(&WorkerOptions{
		Namespace: "{ns1}",
		Queue:     NewRedisQueue(client),
	})
	err := w.Register("success",
		func(*Job, *DequeueOptions) error { return nil },
		&JobOptions{
			MaxExecutionTime: time.Minute,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)
	err = w.Register("failure",
		func(*Job, *DequeueOptions) error { return errors.New("no reason") },
		&JobOptions{
			MaxExecutionTime: time.Minute,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)
	err = w.Register("panic",
		func(*Job, *DequeueOptions) error {
			panic("unexpected")
		},
		&JobOptions{
			MaxExecutionTime: time.Minute,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)

	type message struct {
		Text string
	}
	for i := 0; i < 3; i++ {
		job := NewJob()
		err := job.MarshalPayload(message{Text: "hello"})
		require.NoError(t, err)

		err = w.opt.Queue.Enqueue(job, &EnqueueOptions{
			Namespace: "{ns1}",
			QueueID:   "success",
		})
		require.NoError(t, err)
	}

	count, err := client.ZCard(context.Background(), "{ns1}:queue:success").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	w.Start()
	err = waitEmpty(client, "{ns1}:queue:success", 10*time.Second)
	require.NoError(t, err)
	w.Stop()

	count, err = client.ZCard(context.Background(), "{ns1}:queue:success").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	for i := 0; i < 3; i++ {
		job := NewJob()
		err := job.MarshalPayload(message{Text: "hello"})
		require.NoError(t, err)

		err = w.opt.Queue.Enqueue(job, &EnqueueOptions{
			Namespace: "{ns1}",
			QueueID:   "failure",
		})
		require.NoError(t, err)
	}

	count, err = client.ZCard(context.Background(), "{ns1}:queue:failure").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	w.Start()
	err = waitEmpty(client, "{ns1}:queue:failure", 10*time.Second)
	require.NoError(t, err)
	w.Stop()

	count, err = client.ZCard(context.Background(), "{ns1}:queue:failure").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	for i := 0; i < 3; i++ {
		job, err := NewRedisQueue(client).Dequeue(&DequeueOptions{
			Namespace:    "{ns1}",
			QueueID:      "failure",
			At:           time.Now().Add(time.Hour),
			InvisibleSec: 3600,
		})
		require.NoError(t, err)
		require.True(t, job.Retries > 0)
		require.Equal(t, "no reason", job.LastError)
	}

	for i := 0; i < 3; i++ {
		job := NewJob()
		err := job.MarshalPayload(message{Text: "hello"})
		require.NoError(t, err)

		err = w.opt.Queue.Enqueue(job, &EnqueueOptions{
			Namespace: "{ns1}",
			QueueID:   "panic",
		})
		require.NoError(t, err)
	}

	count, err = client.ZCard(context.Background(), "{ns1}:queue:panic").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	w.Start()
	err = waitEmpty(client, "{ns1}:queue:panic", 10*time.Second)
	require.NoError(t, err)
	w.Stop()

	count, err = client.ZCard(context.Background(), "{ns1}:queue:panic").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	for i := 0; i < 3; i++ {
		job, err := NewRedisQueue(client).Dequeue(&DequeueOptions{
			Namespace:    "{ns1}",
			QueueID:      "panic",
			At:           time.Now().Add(time.Hour),
			InvisibleSec: 3600,
		})
		require.NoError(t, err)
		require.True(t, job.Retries > 0)
		require.True(t, strings.HasPrefix(job.LastError, "panic: unexpected"))
	}
}

func TestWorkerRunOnce(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	job := NewJob()
	err := NewRedisQueue(client).Enqueue(job, &EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "success",
	})
	require.NoError(t, err)

	count, err := client.ZCard(context.Background(), "{ns1}:queue:success").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	job2 := NewJob()
	err = NewRedisQueue(client).Enqueue(job2, &EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "failure",
	})
	require.NoError(t, err)

	count, err = client.ZCard(context.Background(), "{ns1}:queue:failure").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	job3 := NewJob()
	err = NewRedisQueue(client).Enqueue(job3, &EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "panic",
	})
	require.NoError(t, err)

	count, err = client.ZCard(context.Background(), "{ns1}:queue:panic").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	w := NewWorker(&WorkerOptions{
		Namespace: "{ns1}",
		Queue:     NewRedisQueue(client),
	})

	err = w.RunOnce(
		context.Background(),
		"success",
		func(context.Context, *Job, *DequeueOptions) error { return nil },
		&OnceJobOptions{
			MaxExecutionTime: time.Minute,
		},
	)
	require.NoError(t, err)

	count, err = client.ZCard(context.Background(), "{ns1}:queue:success").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	err = w.RunOnce(
		context.Background(),
		"failure",
		func(context.Context, *Job, *DequeueOptions) error { return errors.New("no reason") },
		&OnceJobOptions{
			MaxExecutionTime: time.Minute,
		},
	)
	require.Error(t, err)
	require.Equal(t, "no reason", err.Error())

	count, err = client.ZCard(context.Background(), "{ns1}:queue:failure").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	err = w.RunOnce(
		context.Background(),
		"panic",
		func(context.Context, *Job, *DequeueOptions) error { panic("unexpected") },
		&OnceJobOptions{
			MaxExecutionTime: time.Minute,
		},
	)
	require.Error(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "panic: unexpected"))

	count, err = client.ZCard(context.Background(), "{ns1}:queue:panic").Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestWrappedHandlerError(t *testing.T) {
	errInner := errors.New("test")
	errOuter := &wrappedHandlerError{Err: errInner}
	require.True(t, errors.Is(errOuter, errInner))
}

func TestRetry(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	job := NewJob()
	opt := &DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "q1",
		InvisibleSec: 10,
	}
	retrier := retry(NewRedisQueue(client), defaultBackoff())
	h := retrier(func(*Job, *DequeueOptions) error {
		return ErrUnrecoverable
	})
	err := h(job, opt)
	require.NoError(t, err)

	require.EqualValues(t, 0, job.Retries)
	require.Equal(t, "", job.LastError)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)

	h = retrier(func(*Job, *DequeueOptions) error {
		return fmt.Errorf("recoverable, but not retried: %w", ErrDoNotRetry)
	})
	err = h(job, opt)
	require.Error(t, err)

	require.EqualValues(t, 0, job.Retries)
	require.Equal(t, "", job.LastError)

	var delays []int64
	for i := 1; i <= 10; i++ {
		retryErr := fmt.Errorf("error %d", i)
		h = retrier(func(*Job, *DequeueOptions) error {
			return retryErr
		})
		err = h(job, opt)
		require.Error(t, err)
		require.Equal(t, retryErr, err)

		require.EqualValues(t, i, job.Retries)
		require.Equal(t, retryErr.Error(), job.LastError)

		z, err := client.ZRangeByScoreWithScores(
			context.Background(),
			"{ns1}:queue:q1",
			&redis.ZRangeBy{
				Min: "-inf",
				Max: "+inf",
			}).Result()
		require.NoError(t, err)
		require.Len(t, z, 1)
		require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

		delays = append(delays, job.EnqueuedAt.Unix()-time.Now().Unix())
	}

	t.Log("delay", delays)
	for i := 2; i < len(delays); i += 2 {
		require.True(t, delays[i] > delays[i-2])
		require.True(t, delays[i] > 1)
	}
}
