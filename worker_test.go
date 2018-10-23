package work

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/require"
)

func TestWorkerStartStop(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())

	w := NewWorker(&WorkerOptions{
		Namespace: "ns1",
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

func waitEmpty(client *redis.Client, key string, timeout time.Duration) error {
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutTimer.C:
			return errors.New("timeout")
		case <-ticker.C:
			z, err := client.ZRangeByScoreWithScores(key,
				redis.ZRangeBy{
					Min: "-inf",
					Max: fmt.Sprint(time.Now().Unix()),
				}).Result()
			if err != nil {
				return err
			}
			if len(z) == 0 {
				return nil
			}
		}
	}
}

func TestWorkerRunJobMultiQueue(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())

	type message struct {
		Text string
	}

	w := NewWorker(&WorkerOptions{
		Namespace: "ns1",
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
			MaxExecutionTime: 60 * time.Second,
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
			MaxExecutionTime: 60 * time.Second,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		job := NewJob()
		err := job.MarshalPayload(message{Text: "test1"})
		require.NoError(t, err)

		err = w.Enqueue("test1", job)
		require.NoError(t, err)
	}

	for i := 0; i < 3; i++ {
		job := NewJob()
		err := job.MarshalPayload(message{Text: "test2"})
		require.NoError(t, err)

		err = w.Enqueue("test2", job)
		require.NoError(t, err)
	}

	count, err := client.ZCard("ns1:queue:test1").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	count, err = client.ZCard("ns1:queue:test2").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	w.Start()
	err = waitEmpty(client, "ns1:queue:test1", 10*time.Second)
	require.NoError(t, err)
	err = waitEmpty(client, "ns1:queue:test2", 10*time.Second)
	require.NoError(t, err)
	w.Stop()

	count, err = client.ZCard("ns1:queue:test1").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	count, err = client.ZCard("ns1:queue:test2").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, count)
}

func TestWorkerRunJob(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())

	w := NewWorker(&WorkerOptions{
		Namespace: "ns1",
		Queue:     NewRedisQueue(client),
	})
	err := w.Register("success",
		func(*Job, *DequeueOptions) error { return nil },
		&JobOptions{
			MaxExecutionTime: 60 * time.Second,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)
	err = w.Register("failure",
		func(*Job, *DequeueOptions) error { return errors.New("no reason") },
		&JobOptions{
			MaxExecutionTime: 60 * time.Second,
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

		err = w.Enqueue("success", job)
		require.NoError(t, err)
	}

	count, err := client.ZCard("ns1:queue:success").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	w.Start()
	err = waitEmpty(client, "ns1:queue:success", 10*time.Second)
	require.NoError(t, err)
	w.Stop()

	count, err = client.ZCard("ns1:queue:success").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	for i := 0; i < 3; i++ {
		job := NewJob()
		err := job.MarshalPayload(message{Text: "hello"})
		require.NoError(t, err)

		err = w.Enqueue("failure", job)
		require.NoError(t, err)
	}

	count, err = client.ZCard("ns1:queue:failure").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	w.Start()
	err = waitEmpty(client, "ns1:queue:failure", 10*time.Second)
	require.NoError(t, err)
	w.Stop()

	count, err = client.ZCard("ns1:queue:failure").Result()
	require.NoError(t, err)
	require.EqualValues(t, 3, count)

	for i := 0; i < 3; i++ {
		job, err := NewRedisQueue(client).Dequeue(&DequeueOptions{
			Namespace:    "ns1",
			QueueID:      "failure",
			At:           NewTime(time.Now().Add(time.Hour)),
			InvisibleSec: 3600,
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, job.Retries)
		require.Equal(t, "no reason", job.LastError)
	}
}
