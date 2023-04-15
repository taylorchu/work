package sidekiq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work/redistest"
)

func TestPullDequeueStartEmpty(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	q := NewQueue(client)
	now := time.Now()
	err := q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		10,
	).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:123").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

func TestPullDequeueStartNormal(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	err := client.LPush(context.Background(), "{sidekiq}:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	q := NewQueue(client)
	now := time.Now()
	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		10,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:123").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)
}

func TestPullDequeueStartAlreadyStarted(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	err := client.LPush(context.Background(), "{sidekiq}:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	q := NewQueue(client)
	now := time.Now()
	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		10,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:123").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	err = client.LPush(context.Background(), "{sidekiq}:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)
	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		10,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:123").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestPullDequeueStartRecoveredNotExpired(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	err := client.LPush(context.Background(), "{sidekiq}:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	q := NewQueue(client)
	now := time.Now()
	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		10,
	).Err()
	require.NoError(t, err)

	err = client.LPush(context.Background(), "{sidekiq}:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:123").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"456",
		now.Unix()+1,
		10,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:123").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:456").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 2)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)
	require.Equal(t, "456", z[1].Member)
	require.EqualValues(t, now.Unix()+11, z[1].Score)
}

func TestPullDequeueStartRecoveredExpired(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	err := client.LPush(context.Background(), "{sidekiq}:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	q := NewQueue(client)
	now := time.Now()
	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		10,
	).Err()
	require.NoError(t, err)

	err = client.LPush(context.Background(), "{sidekiq}:queue:default", `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:123").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"456",
		now.Unix()+30,
		10,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), "{sidekiq}:queue:default").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:123").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), "{sidekiq}:sidekiq-queue-pull:default:456").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "456", z[0].Member)
	require.EqualValues(t, now.Unix()+40, z[0].Score)
}

func TestPullDequeueStop(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	q := NewQueue(client)
	now := time.Now()
	err := q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		10,
	).Err()
	require.NoError(t, err)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)

	err = q.(*sidekiqQueue).dequeueStopScript.Run(context.Background(), client, nil,
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
	).Err()
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)
}

func TestPullDequeueHeartbeatNormal(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	q := NewQueue(client)
	now := time.Now()
	err := q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, nil,
		"{sidekiq}",
		"default",
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		10,
	).Err()
	require.NoError(t, err)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)

	err = q.(*sidekiqQueue).dequeueHeartbeatScript.Run(context.Background(), client, nil,
		"{sidekiq}:sidekiq-queue-pull:default",
		"123",
		now.Unix(),
		100,
	).Err()
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+100, z[0].Score)

	err = q.(*sidekiqQueue).dequeueHeartbeatScript.Run(context.Background(), client, nil,
		"{sidekiq}:sidekiq-queue-pull:default",
		"456",
		now.Unix(),
		100,
	).Err()
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		"{sidekiq}:sidekiq-queue-pull:default:pullers",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+100, z[0].Score)
}
