package work

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/require"
)

func TestRedisQueueEnqueue(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
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
		At:        job.CreatedAt,
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	h, err := client.HGetAll(jobKey).Result()
	require.NoError(t, err)
	jobm, err := json.Marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"job": string(jobm),
	}, h)

	z, err := client.ZRangeByScoreWithScores("ns1:queue",
		redis.ZRangeBy{
			Min: fmt.Sprint(job.CreatedAt.Unix()),
			Max: fmt.Sprint(job.CreatedAt.Unix()),
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
}

func TestRedisQueueDequeue(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
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
		At:        job.CreatedAt,
	})
	require.NoError(t, err)

	jobDequeued, err := q.Dequeue(&DequeueOptions{
		Namespace: "ns1",
		At:        job.CreatedAt,
		LockedSec: 60,
	})
	require.NoError(t, err)
	require.Equal(t, job, jobDequeued)

	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	h, err := client.HGetAll(jobKey).Result()
	require.NoError(t, err)
	jobm, err := json.Marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"job": string(jobm),
	}, h)

	z, err := client.ZRangeByScoreWithScores("ns1:queue",
		redis.ZRangeBy{
			Min: fmt.Sprint(job.CreatedAt.Unix() + 60),
			Max: fmt.Sprint(job.CreatedAt.Unix() + 60),
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)

	// empty
	_, err = q.Dequeue(&DequeueOptions{
		Namespace: "ns1",
		At:        job.CreatedAt,
	})
	require.Error(t, err)
	require.Equal(t, "empty", err.Error())
}

func TestRedisQueueAck(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	require.NoError(t, client.FlushAll().Err())
	q := NewRedisQueue(client)

	job := NewJob()

	err := q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		At:        job.CreatedAt,
	})
	require.NoError(t, err)

	z, err := client.ZRangeByScoreWithScores("ns1:queue",
		redis.ZRangeBy{
			Min: fmt.Sprint(job.CreatedAt.Unix()),
			Max: fmt.Sprint(job.CreatedAt.Unix()),
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)

	err = q.Ack(job, &DequeueOptions{Namespace: "ns1"})
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores("ns1:queue",
		redis.ZRangeBy{
			Min: fmt.Sprint(job.CreatedAt.Unix()),
			Max: fmt.Sprint(job.CreatedAt.Unix()),
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)
}
