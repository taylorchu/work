package work

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work/redistest"
)

func TestRedisQueueEnqueue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	type message struct {
		Text string
	}

	job := NewJob()
	err := job.MarshalPayload(message{Text: "hello"})
	require.NoError(t, err)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)

	h, err := client.HGetAll(context.Background(), jobKey).Result()
	require.NoError(t, err)
	jobm, err := marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"allow_promotion": "0",
		"msgpack":          string(jobm),
	}, h)

	jobs, err := q.BulkFind([]string{job.ID, "not-exist-id"}, &FindOptions{
		Namespace: "{ns-work}",
	})
	require.NoError(t, err)
	require.Len(t, jobs, 2)
	require.Equal(t, job.ID, jobs[0].ID)
	require.Equal(t, "", jobs[0].LastError)
	require.Nil(t, jobs[1])

	jobs[0].LastError = "hello world"
	err = q.Enqueue(jobs[0], &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)
	jobs, err = q.BulkFind([]string{job.ID}, &FindOptions{
		Namespace: "{ns-work}",
	})
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, job.ID, jobs[0].ID)
	require.Equal(t, "hello world", jobs[0].LastError)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns-work}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	err = q.Enqueue(job.Delay(time.Minute), &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns-work}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix()+60, z[0].Score)
}

func TestRedisQueueDequeue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	type message struct {
		Text string
	}

	job := NewJob()
	err := job.MarshalPayload(message{Text: "hello"})
	require.NoError(t, err)
	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	now := job.EnqueuedAt.Add(123 * time.Second)

	jobDequeued, err := q.Dequeue(&DequeueOptions{
		Namespace:    "{ns-work}",
		QueueID:      "q1",
		At:           now,
		InvisibleSec: 0,
	})
	require.NoError(t, err)
	require.Equal(t, job, jobDequeued)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns-work}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, job.EnqueuedAt.Unix(), z[0].Score)

	jobDequeued, err = q.Dequeue(&DequeueOptions{
		Namespace:    "{ns-work}",
		QueueID:      "q1",
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
		"allow_promotion": "0",
		"msgpack":          string(jobm),
	}, h)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns-work}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, jobKey, z[0].Member)
	require.EqualValues(t, now.Unix()+60, z[0].Score)

	// empty
	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "{ns-work}",
		QueueID:      "q1",
		At:           now,
		InvisibleSec: 60,
	})
	require.Error(t, err)
	require.Equal(t, ErrEmptyQueue, err)
}

func TestRedisQueueDequeueDeletedJob(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	type message struct {
		Text string
	}

	job := NewJob()
	err := job.MarshalPayload(message{Text: "hello"})
	require.NoError(t, err)

	err = q.Enqueue(job, &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)

	h, err := client.HGetAll(context.Background(), jobKey).Result()
	require.NoError(t, err)
	jobm, err := marshal(job)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"allow_promotion": "0",
		"msgpack":          string(jobm),
	}, h)

	require.NoError(t, client.Del(context.Background(), jobKey).Err())

	_, err = q.Dequeue(&DequeueOptions{
		Namespace:    "{ns-work}",
		QueueID:      "q1",
		At:           job.EnqueuedAt,
		InvisibleSec: 60,
	})
	require.Equal(t, ErrEmptyQueue, err)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns-work}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)
}

func TestRedisQueueAck(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	job := NewJob()

	err := q.Enqueue(job, &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns-work}:queue:q1",
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

	err = q.Ack(job, &AckOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns-work}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)

	e, err = client.Exists(context.Background(), jobKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, e)

	err = q.Ack(job, &AckOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)
}

func TestRedisQueueGetQueueMetrics(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	job := NewJob()

	err := q.Enqueue(job, &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	m, err := q.GetQueueMetrics(&QueueMetricsOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
		At:        job.EnqueuedAt,
	})
	require.NoError(t, err)
	require.Equal(t, "{ns-work}", m.Namespace)
	require.Equal(t, "q1", m.QueueID)
	require.EqualValues(t, 1, m.ReadyTotal)
	require.EqualValues(t, 0, m.ScheduledTotal)
	require.True(t, 0 < m.Latency && m.Latency < time.Minute)

	m, err = q.GetQueueMetrics(&QueueMetricsOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
		At:        job.EnqueuedAt.Add(-time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, "{ns-work}", m.Namespace)
	require.Equal(t, "q1", m.QueueID)
	require.EqualValues(t, 0, m.ReadyTotal)
	require.EqualValues(t, 1, m.ScheduledTotal)
	require.True(t, m.Latency == 0)
}

func TestRedisQueueBulkEnqueue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	const jobCount = 100000
	var jobs []*Job
	for i := 0; i < jobCount; i++ {
		job := NewJob()
		jobs = append(jobs, job)
	}

	err := q.BulkEnqueue(jobs, &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	})
	require.NoError(t, err)

	count, err := client.ZCard(context.Background(), "{ns-work}:queue:q1").Result()
	require.NoError(t, err)
	require.Equal(t, int64(jobCount), count)
}

func TestRedisQueuePromoteJob(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	// Enqueue two jobs with old timestamps (in the past)
	job1 := NewJob()
	job1.EnqueuedAt = time.Now().Add(-time.Hour) // 1 hour ago
	job2 := NewJob()
	job2.EnqueuedAt = time.Now().Add(-time.Hour) // 1 hour ago

	opts := &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	}

	err := q.Enqueue(job1, opts)
	require.NoError(t, err)
	err = q.Enqueue(job2, opts)
	require.NoError(t, err)

	// Check initial score of job2 (should be old timestamp)
	queueKey := "{ns-work}:queue:q1"
	jobKey := fmt.Sprintf("{ns-work}:job:%s", job2.ID)
	initialScore, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.Equal(t, float64(job2.EnqueuedAt.Unix()), initialScore)

	// Promote job2
	beforePromote := time.Now().Unix()
	err = q.PromoteJob(job2.ID, &PromoteOptions{
		Namespace: opts.Namespace,
		QueueID:   opts.QueueID,
	})
	require.NoError(t, err)
	afterPromote := time.Now().Unix()

	// Check that job2's score was updated to now
	newScore, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, int64(newScore), beforePromote)
	require.LessOrEqual(t, int64(newScore), afterPromote)

	// Promote non-existent job should not error (XX flag prevents adding)
	err = q.PromoteJob("non-existent-job", &PromoteOptions{
		Namespace: opts.Namespace,
		QueueID:   opts.QueueID,
	})
	require.NoError(t, err)

	// Verify non-existent job was not added to queue
	exists, err := client.ZScore(context.Background(), queueKey, "{ns-work}:job:non-existent-job").Result()
	require.Error(t, err) // redis.Nil error expected
	require.Equal(t, float64(0), exists)
}

func TestRedisQueueEnqueueGuardDoesNotDemote(t *testing.T) {
	// AllowPromotion defaults to false. A second Enqueue of the same job
	// with an earlier score must be a no-op so deterministic-ID dedup
	// jobs cannot have their deferred run time clobbered.
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	opts := &EnqueueOptions{Namespace: "{ns-work}", QueueID: "q1"}

	job := NewJob()
	deferredAt := time.Now().Add(time.Minute)
	job.EnqueuedAt = deferredAt
	require.NoError(t, q.Enqueue(job, opts))

	queueKey := "{ns-work}:queue:q1"
	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)
	scoreAfterFirst, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, deferredAt.Unix(), scoreAfterFirst)

	// Re-enqueue the same ID with an earlier score (the dedup pattern).
	job.EnqueuedAt = time.Now()
	job.AllowPromotion = true
	require.NoError(t, q.Enqueue(job, opts))

	scoreAfterSecond, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.Equal(t, scoreAfterFirst, scoreAfterSecond, "GT must reject the earlier-score re-enqueue when AllowPromotion is false")

	require.NoError(t, q.PromoteJob(job.ID, &PromoteOptions{
		Namespace: opts.Namespace,
		QueueID:   opts.QueueID,
	}))

	scoreAfterPromote, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.Equal(t, scoreAfterFirst, scoreAfterPromote, "no-op re-enqueue must not flip AllowPromotion")
}

func TestRedisQueueEnqueueAllowPromotionDemotes(t *testing.T) {
	// AllowPromotion=true opts the job out of the GT guard so an explicit
	// opt-in retry flow can lower the score from the Dequeue InvisibleSec
	// mark back down to now + backoff.
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	opts := &EnqueueOptions{Namespace: "{ns-work}", QueueID: "q1"}

	job := NewJob()
	job.AllowPromotion = true
	deferredAt := time.Now().Add(time.Minute)
	job.EnqueuedAt = deferredAt
	require.NoError(t, q.Enqueue(job, opts))

	queueKey := "{ns-work}:queue:q1"
	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)

	// Re-enqueue the same ID with an earlier score.
	earlierAt := time.Now()
	job.EnqueuedAt = earlierAt
	require.NoError(t, q.Enqueue(job, opts))

	scoreAfter, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, earlierAt.Unix(), scoreAfter, "plain ZADD must lower the score when AllowPromotion is true")
}

func TestRedisQueueBulkEnqueueMixedAllowPromotion(t *testing.T) {
	// A single BulkEnqueue containing both guarded (AllowPromotion=false)
	// and promotable (AllowPromotion=true) jobs must apply the right
	// semantic to each, atomically.
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	opts := &EnqueueOptions{Namespace: "{ns-work}", QueueID: "q1"}

	guarded := NewJob()
	deferredAt := time.Now().Add(time.Minute)
	guarded.EnqueuedAt = deferredAt

	promotable := NewJob()
	promotable.AllowPromotion = true
	promotable.EnqueuedAt = deferredAt

	require.NoError(t, q.BulkEnqueue([]*Job{guarded, promotable}, opts))

	// Re-enqueue both with an earlier score.
	earlierAt := time.Now()
	guarded.EnqueuedAt = earlierAt
	promotable.EnqueuedAt = earlierAt
	require.NoError(t, q.BulkEnqueue([]*Job{guarded, promotable}, opts))

	queueKey := "{ns-work}:queue:q1"

	guardedScore, err := client.ZScore(context.Background(), queueKey, fmt.Sprintf("{ns-work}:job:%s", guarded.ID)).Result()
	require.NoError(t, err)
	require.EqualValues(t, deferredAt.Unix(), guardedScore, "guarded job must retain its later score")

	promotableScore, err := client.ZScore(context.Background(), queueKey, fmt.Sprintf("{ns-work}:job:%s", promotable.ID)).Result()
	require.NoError(t, err)
	require.EqualValues(t, earlierAt.Unix(), promotableScore, "promotable job must accept the earlier score")
}

func TestRedisQueuePromoteJobAllowPromotionDemotes(t *testing.T) {
	// PromoteJob on a job with AllowPromotion=true must be able to advance
	// the score even when the job is currently sitting at the InvisibleSec
	// mark from a prior Dequeue — that is precisely the state the subqueue
	// PromoteOnAck path needs to advance.
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	opts := &EnqueueOptions{Namespace: "{ns-work}", QueueID: "q1"}

	job := NewJob()
	job.AllowPromotion = true
	job.EnqueuedAt = time.Now()
	require.NoError(t, q.Enqueue(job, opts))

	dequeued, err := q.Dequeue(&DequeueOptions{
		Namespace:    "{ns-work}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60,
	})
	require.NoError(t, err)
	require.Equal(t, job.ID, dequeued.ID)

	queueKey := "{ns-work}:queue:q1"
	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)
	scoreAfterDequeue, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)

	beforePromote := time.Now().Unix()
	require.NoError(t, q.PromoteJob(job.ID, &PromoteOptions{
		Namespace: opts.Namespace,
		QueueID:   opts.QueueID,
	}))

	scoreAfterPromote, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.Less(t, int64(scoreAfterPromote), int64(scoreAfterDequeue), "PromoteJob must lower the score for AllowPromotion=true jobs")
	require.GreaterOrEqual(t, int64(scoreAfterPromote), beforePromote)
}

func TestRedisQueuePromoteJobDoesNotDemote(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	// Enqueue a job
	job := NewJob()
	job.EnqueuedAt = time.Now()

	opts := &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	}

	err := q.Enqueue(job, opts)
	require.NoError(t, err)

	// Dequeue the job (this sets score to now + invisibleSec)
	dequeueOpts := &DequeueOptions{
		Namespace:    "{ns-work}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60, // 60 seconds
	}
	dequeuedJob, err := q.Dequeue(dequeueOpts)
	require.NoError(t, err)
	require.Equal(t, job.ID, dequeuedJob.ID)

	// Check that job's score is now + invisibleSec
	queueKey := "{ns-work}:queue:q1"
	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)
	scoreAfterDequeue, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)

	// Try to promote the job (should not demote it because of GT flag)
	err = q.PromoteJob(job.ID, &PromoteOptions{
		Namespace: opts.Namespace,
		QueueID:   opts.QueueID,
	})
	require.NoError(t, err)

	// Verify score hasn't changed (GT flag prevented demotion)
	scoreAfterPromote, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.Equal(t, scoreAfterDequeue, scoreAfterPromote)
}

func TestRedisQueuePromoteJobMissingAllowPromotionDoesNotDemote(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client, "{ns-work}"))
	q := NewRedisQueue(client)

	job := NewJob()
	job.EnqueuedAt = time.Now()

	opts := &EnqueueOptions{
		Namespace: "{ns-work}",
		QueueID:   "q1",
	}

	require.NoError(t, q.Enqueue(job, opts))

	jobKey := fmt.Sprintf("{ns-work}:job:%s", job.ID)
	require.NoError(t, client.HDel(context.Background(), jobKey, "allow_promotion").Err())

	dequeuedJob, err := q.Dequeue(&DequeueOptions{
		Namespace:    "{ns-work}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60,
	})
	require.NoError(t, err)
	require.Equal(t, job.ID, dequeuedJob.ID)

	queueKey := "{ns-work}:queue:q1"
	scoreAfterDequeue, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)

	require.NoError(t, q.PromoteJob(job.ID, &PromoteOptions{
		Namespace: opts.Namespace,
		QueueID:   opts.QueueID,
	}))

	scoreAfterPromote, err := client.ZScore(context.Background(), queueKey, jobKey).Result()
	require.NoError(t, err)
	require.Equal(t, scoreAfterDequeue, scoreAfterPromote)
}
