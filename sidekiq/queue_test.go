package sidekiq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redistest"
)

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

func TestSidekiqQueueExternalBulkEnqueue(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := NewQueue(client)

	const jobCount = 100000
	var jobs []*work.Job
	for i := 0; i < jobCount; i++ {
		job := work.NewJob()
		err := job.MarshalJSONPayload([]int{1, 2, 3})
		require.NoError(t, err)
		jobs = append(jobs, job)
	}

	err := q.ExternalBulkEnqueue(jobs, &work.EnqueueOptions{
		Namespace: "{sidekiq}",
		QueueID:   "import/TestWorker",
	})
	require.NoError(t, err)

	count, err := client.LLen(context.Background(), "{sidekiq}:queue:import").Result()
	require.NoError(t, err)
	require.Equal(t, int64(jobCount), count)
}
