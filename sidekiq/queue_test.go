package sidekiq

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redislock"
	"github.com/taylorchu/work/redistest"
	"github.com/vmihailenco/msgpack/v5"
)

func marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)
	enc.UseCompactFloats(true)
	enc.SetSortMapKeys(true)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func TestSidekiqQueueExternalEnqueue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	q := NewQueue(client)
	job := work.NewJob()
	job.ID = "0e821cf2-d0cc-11e9-92f2-d059e4b80cfc"
	job.CreatedAt = time.Unix(1567791042, 0)
	job.UpdatedAt = time.Unix(1567791043, 0)
	job.EnqueuedAt = time.Unix(1567791044, 0)
	job.LastError = "error: test"
	job.Retries = 2

	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.ExternalEnqueue(job, &work.EnqueueOptions{
		Namespace: "{sidekiq}",
		QueueID:   "import/TestWorker",
	})
	require.NoError(t, err)

	l, err := client.LRange(context.Background(), "{sidekiq}:queue:import", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, l, 1)

	require.Equal(t, `{"class":"TestWorker","jid":"0e821cf2-d0cc-11e9-92f2-d059e4b80cfc","args":[1,2,3],"created_at":1567791042,"enqueued_at":1567791044,"queue":"import","retry":true,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791043}`, l[0])
}

func TestSidekiqQueueExternalEnqueueScheduled(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	q := NewQueue(client)
	job := work.NewJob()
	job.ID = "0e821cf2-d0cc-11e9-92f2-d059e4b80cfc"
	job.CreatedAt = time.Unix(91567791042, 0)
	job.UpdatedAt = time.Unix(91567791043, 0)
	job.EnqueuedAt = time.Unix(91567791044, 0)
	job.LastError = "error: test"
	job.Retries = 2

	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.ExternalEnqueue(job, &work.EnqueueOptions{
		Namespace: "{sidekiq}",
		QueueID:   "import/TestWorker",
	})
	require.NoError(t, err)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:schedule",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, `{"class":"TestWorker","jid":"0e821cf2-d0cc-11e9-92f2-d059e4b80cfc","args":[1,2,3],"created_at":91567791042,"enqueued_at":91567791044,"queue":"import","retry":true,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":91567791043,"retried_at":91567791043}`, z[0].Member)
	require.EqualValues(t, 91567791044, z[0].Score)
}

func TestSidekiqQueueExternalDequeue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	err := client.LPush(context.Background(), "{sidekiq}:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	q := NewQueue(client)
	err = q.Pull(&PullOptions{
		Namespace:        "{sidekiq}",
		SidekiqNamespace: "{sidekiq}",
		SidekiqQueue:     "default",
	})
	require.NoError(t, err)
	job, err := q.Dequeue(&work.DequeueOptions{
		Namespace:    "{sidekiq}",
		QueueID:      "default/TestWorker",
		At:           time.Now(),
		InvisibleSec: 0,
	})
	require.NoError(t, err)
	require.Equal(t, "83b27ea26dd65821239ca6aa", job.ID)
	require.EqualValues(t, 1567788641, job.CreatedAt.Unix())
	require.EqualValues(t, 1567791046, job.UpdatedAt.Unix())
	require.EqualValues(t, 1567788642, job.EnqueuedAt.Unix())
	require.EqualValues(t, 2, job.Retries)
	require.Equal(t, "error: test", job.LastError)
	require.EqualValues(t, "[]", job.Payload)
}

func TestSidekiqQueueEnqueue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("{ns1}:job:%s", job.ID)

	h, err := client.HGetAll(context.Background(), jobKey).Result()
	require.NoError(t, err)
	jobm, err := marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"msgpack": string(jobm),
	}, h)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	err = q.Enqueue(job.Delay(time.Minute), &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix()+60, z[0].Score)
}

func TestSidekiqQueueDequeue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)
	jobKey := fmt.Sprintf("{ns1}:job:%s", job.ID)

	err = q.ExternalEnqueue(job, &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	now := job.EnqueuedAt.Add(123 * time.Second)

	err = q.schedule("{ns1}", now)
	require.NoError(t, err)
	err = q.Pull(&PullOptions{
		Namespace:        "{ns1}",
		SidekiqNamespace: "{ns1}",
		SidekiqQueue:     "low",
	})
	require.NoError(t, err)
	jobDequeued, err := q.Dequeue(&work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "low/q1",
		At:           now,
		InvisibleSec: 0,
	})
	require.NoError(t, err)
	require.Equal(t, job, jobDequeued)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	err = q.schedule("{ns1}", now)
	require.NoError(t, err)
	jobDequeued, err = q.Dequeue(&work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "low/q1",
		At:           now,
		InvisibleSec: 60,
	})
	require.NoError(t, err)
	require.Equal(t, job, jobDequeued)

	h, err := client.HGetAll(context.Background(), jobKey).Result()
	require.NoError(t, err)
	jobm, err := marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"msgpack": string(jobm),
	}, h)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, now.Unix()+60, z[0].Score)

	// empty
	err = q.schedule("{ns1}", now)
	require.NoError(t, err)
	_, err = q.Dequeue(&work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "low/q1",
		At:           now,
		InvisibleSec: 60,
	})
	require.Error(t, err)
	require.Equal(t, work.ErrEmptyQueue, err)
}

func TestSidekiqQueueDequeueLocked(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.ExternalEnqueue(job, &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	now := job.EnqueuedAt.Add(123 * time.Second)

	err = q.schedule("{ns1}", now)
	require.NoError(t, err)

	lock := &redislock.Lock{
		Client:       client,
		Key:          "{ns1}:sidekiq-queue-pull:low",
		ID:           uuid.NewString(),
		At:           time.Now(),
		ExpireInSec:  30,
		MaxAcquirers: 1,
	}
	acquired, err := lock.Acquire()
	require.NoError(t, err)
	require.True(t, acquired)

	err = q.Pull(&PullOptions{
		Namespace:        "{ns1}",
		SidekiqNamespace: "{ns1}",
		SidekiqQueue:     "low",
	})
	require.NoError(t, err)
}

func TestSidekiqQueueDequeueDeletedJob(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("{ns1}:job:%s", job.ID)

	h, err := client.HGetAll(context.Background(), jobKey).Result()
	require.NoError(t, err)
	jobm, err := marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"msgpack": string(jobm),
	}, h)

	require.NoError(t, client.Del(context.Background(), jobKey).Err())

	_, err = q.Dequeue(&work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "low/q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 60,
	})
	require.Equal(t, work.ErrEmptyQueue, err)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)
}

func TestSidekiqQueueAck(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("{ns1}:job:%s", job.ID)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	e, err := client.Exists(context.Background(), jobKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, e)

	err = q.Ack(job, &work.AckOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)

	e, err = client.Exists(context.Background(), jobKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, e)

	err = q.Ack(job, &work.AckOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)
}

func TestSidekiqQueueGetQueueMetrics(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := NewQueue(client)

	job := work.NewJob()
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)

	err = q.Enqueue(job, &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	m, err := q.GetQueueMetrics(&work.QueueMetricsOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
		At:        job.EnqueuedAt,
	})
	require.NoError(t, err)
	require.Equal(t, "{ns1}", m.Namespace)
	require.Equal(t, "low/q1", m.QueueID)
	require.EqualValues(t, 1, m.ReadyTotal)
	require.EqualValues(t, 0, m.ScheduledTotal)
	require.True(t, 0 < m.Latency && m.Latency < time.Minute)

	m, err = q.GetQueueMetrics(&work.QueueMetricsOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
		At:        job.EnqueuedAt.Add(-time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, "{ns1}", m.Namespace)
	require.Equal(t, "low/q1", m.QueueID)
	require.EqualValues(t, 0, m.ReadyTotal)
	require.EqualValues(t, 1, m.ScheduledTotal)
	require.True(t, m.Latency == 0)
}

func TestSidekiqQueueEnqueueDuplicated(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := NewQueue(client)

	job := work.NewJob().Delay(time.Minute)
	err := job.MarshalJSONPayload([]int{1, 2, 3})
	require.NoError(t, err)
	jobKey := fmt.Sprintf("{ns1}:job:%s", job.ID)

	err = q.ExternalEnqueue(job, &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	now := job.EnqueuedAt

	err = q.schedule("{ns1}", now)
	require.NoError(t, err)
	err = q.Pull(&PullOptions{
		Namespace:        "{ns1}",
		SidekiqNamespace: "{ns1}",
		SidekiqQueue:     "low",
	})
	require.NoError(t, err)
	jobDequeued, err := q.Dequeue(&work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "low/q1",
		At:           now,
		InvisibleSec: 60,
	})
	require.NoError(t, err)
	require.Equal(t, job, jobDequeued)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix()+60, z[0].Score)

	err = q.ExternalEnqueue(job, &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "low/q1",
	})
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix()+60, z[0].Score)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:schedule",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	// pull again with same job id
	l, err := client.LRange(context.Background(), "{ns1}:queue:low", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, l, 0)

	err = q.schedule("{ns1}", now)
	require.NoError(t, err)

	l, err = client.LRange(context.Background(), "{ns1}:queue:low", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, l, 1)

	err = q.Pull(&PullOptions{
		Namespace:        "{ns1}",
		SidekiqNamespace: "{ns1}",
		SidekiqQueue:     "low",
	})
	require.NoError(t, err)

	l, err = client.LRange(context.Background(), "{ns1}:queue:low", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, l, 0)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:low/q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix()+60, z[0].Score)
}
