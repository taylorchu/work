package work

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/require"
)

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		PoolSize:     10,
		MinIdleConns: 10,
	})
}

func TestRedisQueueEnqueue(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewRedisQueue(client)

	type message struct {
		Text string
	}

	job := NewJob()
	err := job.MarshalPayload(message{Text: "hello"})
	require.NoError(t, err)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "q1",
		At:        job.CreatedAt,
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	h, err := client.HGetAll(jobKey).Result()
	require.NoError(t, err)
	jobm, err := json.Marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"json": string(jobm),
	}, h)

	z, err := client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: fmt.Sprint(job.CreatedAt.Unix()),
			Max: fmt.Sprint(job.CreatedAt.Unix()),
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "q1",
		At:        NewTime(job.CreatedAt.Add(60 * time.Second)),
	})
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
}

func TestRedisQueueDequeue(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewRedisQueue(client)

	type message struct {
		Text string
	}

	job := NewJob()
	err := job.MarshalPayload(message{Text: "hello"})
	require.NoError(t, err)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "q1",
		At:        job.CreatedAt,
	})
	require.NoError(t, err)

	jobDequeued, err := q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "q1",
		At:           job.CreatedAt,
		InvisibleSec: 60,
	})
	require.NoError(t, err)
	require.Equal(t, job, jobDequeued)

	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	h, err := client.HGetAll(jobKey).Result()
	require.NoError(t, err)
	jobm, err := json.Marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"json": string(jobm),
	}, h)

	z, err := client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: fmt.Sprint(job.CreatedAt.Unix() + 60),
			Max: fmt.Sprint(job.CreatedAt.Unix() + 60),
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)

	// empty
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "q1",
		At:           job.CreatedAt,
		InvisibleSec: 60,
	})
	require.Error(t, err)
	require.Equal(t, ErrEmptyQueue, err)
}

func TestRedisQueueAck(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewRedisQueue(client)

	job := NewJob()

	err := q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "q1",
		At:        job.CreatedAt,
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	z, err := client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: fmt.Sprint(job.CreatedAt.Unix()),
			Max: fmt.Sprint(job.CreatedAt.Unix()),
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)

	e, err := client.Exists(jobKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, e)

	err = q.Ack(job, &AckOptions{
		Namespace: "ns1",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)

	e, err = client.Exists(jobKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, e)
}
