package sidekiq

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/vmihailenco/msgpack/v4"
)

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		PoolSize:     10,
		MinIdleConns: 10,
	})
}

func marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactEncoding(true)
	enc.SortMapKeys(true)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func TestSidekiqQueueEnqueueExternal(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())

	q := NewQueue(client)
	job := work.NewJob()
	job.ID = "0e821cf2-d0cc-11e9-92f2-d059e4b80cfc"
	job.CreatedAt = time.Unix(1567791044, 0)
	job.UpdatedAt = time.Unix(1567791044, 0)
	job.EnqueuedAt = time.Unix(1567791044, 0)

	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "sidekiq",
		QueueID:   "import/TestWorker",
	})
	require.NoError(t, err)

	z, err := client.ZRangeByScoreWithScores("sidekiq:schedule",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, `{"class":"TestWorker","jid":"0e821cf2-d0cc-11e9-92f2-d059e4b80cfc","args":[1,2,3],"created_at":1567791044,"enqueued_at":1567791044,"queue":"import","retry":true}`, z[0].Member)
	require.EqualValues(t, 1567791044, z[0].Score)
}

func TestSidekiqQueueDequeueExternal(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	err := client.LPush("sidekiq:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788643.0875323,"enqueued_at":1567788643.0879307}"`).Err()
	require.NoError(t, err)

	q := NewQueue(client)
	job, err := q.Dequeue(&work.DequeueOptions{
		Namespace:    "sidekiq",
		QueueID:      "default/TestWorker",
		At:           time.Now(),
		InvisibleSec: 0,
	})
	require.NoError(t, err)
	require.Equal(t, "83b27ea26dd65821239ca6aa", job.ID)
	require.EqualValues(t, 1567788643, job.CreatedAt.Unix())
	require.EqualValues(t, 1567788643, job.UpdatedAt.Unix())
	require.EqualValues(t, 1567788643, job.EnqueuedAt.Unix())
	require.EqualValues(t, "[]", job.Payload)
}

func TestSidekiqQueueEnqueue(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&work.DequeueOptions{
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

	err = q.Enqueue(job.Delay(time.Minute), &work.EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.Delay(time.Minute).EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&work.DequeueOptions{
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
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)
	jobKey := fmt.Sprintf("ns1:job:%s", job.ID)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	now := job.EnqueuedAt.Add(123 * time.Second)

	err = q.(*sidekiqQueue).schedule("ns1", now)
	require.NoError(t, err)
	jobDequeued, err := q.Dequeue(&work.DequeueOptions{
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
	jobDequeued, err = q.Dequeue(&work.DequeueOptions{
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
	_, err = q.Dequeue(&work.DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           now,
		InvisibleSec: 60,
	})
	require.Error(t, err)
	require.Equal(t, work.ErrEmptyQueue, err)
}

func TestSidekiqQueueDequeueDeletedJob(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&work.DequeueOptions{
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
	_, err = q.Dequeue(&work.DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 60,
	})
	require.Equal(t, work.ErrEmptyQueue, err)

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
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&work.DequeueOptions{
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

	err = q.Ack(job, &work.AckOptions{
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

	err = q.Ack(job, &work.AckOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)
}

func TestSidekiqQueueGetQueueMetrics(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	err = q.(*sidekiqQueue).schedule("ns1", job.EnqueuedAt)
	require.NoError(t, err)
	_, err = q.Dequeue(&work.DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "low/q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 0,
	})
	require.NoError(t, err)

	m, err := q.(work.MetricsExporter).GetQueueMetrics(&work.QueueMetricsOptions{
		Namespace: "ns1",
		QueueID:   "low/q1",
		At:        job.EnqueuedAt,
	})
	require.NoError(t, err)
	require.Equal(t, "ns1", m.Namespace)
	require.Equal(t, "q1", m.QueueID)
	require.EqualValues(t, 1, m.ReadyTotal)
	require.EqualValues(t, 0, m.ScheduledTotal)

	m, err = q.(work.MetricsExporter).GetQueueMetrics(&work.QueueMetricsOptions{
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
