package work

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/require"
)

func TestSidekiqQueueEnqueue(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewSidekiqQueue(client)

	type message struct {
		Text string
	}

	job := NewJob()
	err := job.MarshalPayload(message{Text: "hello"})
	require.NoError(t, err)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 0,
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	h, err := client.HGetAll(jobKey).Result()
	require.NoError(t, err)
	jobm, err := marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"msgpack": string(jobm),
	}, h)

	z, err := client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	err = q.Enqueue(job.Delay(time.Minute), &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.Delay(time.Minute).EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           job.Delay(time.Minute).EnqueuedAt,
		InvisibleSec: 0,
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
	require.EqualValues(t, job.EnqueuedAt.Unix()+60, z[0].Score)
}

func TestSidekiqQueueDequeue(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewSidekiqQueue(client)

	type message struct {
		Text string
	}

	job := NewJob()
	err := job.MarshalPayload(message{Text: "hello"})
	require.NoError(t, err)
	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	now := job.EnqueuedAt.Add(123 * time.Second)

	err = q.(*sidekiqQueue).schedule("ns1", now)
	require.NoError(t, err)
	jobDequeued, err := q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           now,
		InvisibleSec: 0,
	})
	require.NoError(t, err)
	require.Equal(t, job, jobDequeued)

	z, err := client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	err = q.(*sidekiqQueue).schedule("ns1", now)
	require.NoError(t, err)
	jobDequeued, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           now,
		InvisibleSec: 60,
	})
	require.NoError(t, err)
	require.Equal(t, job, jobDequeued)

	h, err := client.HGetAll(jobKey).Result()
	require.NoError(t, err)
	jobm, err := marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"msgpack": string(jobm),
	}, h)

	z, err = client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, now.Unix()+60, z[0].Score)

	// empty
	err = q.(*sidekiqQueue).schedule("ns1", now)
	require.NoError(t, err)
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           now,
		InvisibleSec: 60,
	})
	require.Error(t, err)
	require.Equal(t, ErrEmptyQueue, err)
}

func TestSidekiqQueueDequeueDeletedJob(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewSidekiqQueue(client)

	type message struct {
		Text string
	}

	job := NewJob()
	err := job.MarshalPayload(message{Text: "hello"})
	require.NoError(t, err)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 0,
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	h, err := client.HGetAll(jobKey).Result()
	require.NoError(t, err)
	jobm, err := marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"msgpack": string(jobm),
	}, h)

	require.NoError(t, client.Del(jobKey).Err())

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 60,
	})
	require.Equal(t, ErrEmptyQueue, err)

	z, err := client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)
}

func TestSidekiqQueueAck(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewSidekiqQueue(client)

	job := NewJob()

	err := q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 0,
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	z, err := client.ZRangeByScoreWithScores("ns1:queue:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	e, err := client.Exists(jobKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, e)

	err = q.Ack(job, &AckOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
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

	err = q.Ack(job, &AckOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)
}

func TestSidekiqQueueGetQueueMetrics(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewSidekiqQueue(client)

	job := NewJob()

	err := q.Enqueue(job, &EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 0,
	})
	require.NoError(t, err)

	m, err := q.(MetricsExporter).GetQueueMetrics(&QueueMetricsOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
		At:        job.EnqueuedAt,
	})
	require.NoError(t, err)
	require.Equal(t, "ns1", m.Namespace)
	require.Equal(t, "q1", m.QueueID)
	require.EqualValues(t, 1, m.ReadyTotal)
	require.EqualValues(t, 0, m.ScheduledTotal)

	m, err = q.(MetricsExporter).GetQueueMetrics(&QueueMetricsOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
		At:        job.EnqueuedAt.Add(-time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, "ns1", m.Namespace)
	require.Equal(t, "q1", m.QueueID)
	require.EqualValues(t, 0, m.ReadyTotal)
	require.EqualValues(t, 1, m.ScheduledTotal)
}
