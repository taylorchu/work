package heartbeat

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redistest"
)

func TestHeartbeater(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60,
	}

	hb := Heartbeater(&HeartbeaterOptions{
		Queue:        work.NewRedisQueue(client),
		InvisibleSec: 30,
		IntervalSec:  10,
	})

	h := hb(func(*work.Job, *work.DequeueOptions) error {
		return nil
	})

	err := h(job, opt)
	require.NoError(t, err)
	require.Equal(t, job.CreatedAt, job.UpdatedAt)
	require.Equal(t, job.CreatedAt, job.EnqueuedAt)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.EqualValues(t, job.EnqueuedAt.Unix()+30, z[0].Score)

	err = h(job, opt)
	require.NoError(t, err)
	require.Equal(t, job.CreatedAt, job.UpdatedAt)
	require.Equal(t, job.CreatedAt, job.EnqueuedAt)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{ns1}:queue:q1",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.EqualValues(t, job.EnqueuedAt.Unix()+30, z[0].Score)
}
