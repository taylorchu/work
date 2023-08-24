package sidekiq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work/redistest"
)

const (
	pullTestSidekiqNamespace = "{sidekiq}"
	pullTestSidekiqQueue     = "default"
	pullTestSidekiqQueueKey  = "{sidekiq}:queue:default"
	pullTestNamespace        = "{sidekiq}:sidekiq-queue-pull:default"
	pullTestPullersKey       = "{sidekiq}:sidekiq-queue-pull:default:pullers"
)

func TestPullDequeueStartEmpty(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	q := NewQueue(client)
	now := time.Now()

	err := q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		1,
	).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

func TestPullDequeueStartNormal(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	err := client.LPush(context.Background(), pullTestSidekiqQueueKey, `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	q := NewQueue(client)
	now := time.Now()

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		1,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)
}

func TestPullDequeueStartNormalFirstN(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	jobIDs := []string{"83b27ea26dd65821239ca6aa", "ebb3186ec09c42642f980a20", "7e59de95478191698aa69b22"}
	for _, jobID := range jobIDs {
		err := client.LPush(context.Background(), pullTestSidekiqQueueKey, fmt.Sprintf(`{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":%q,"created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`, jobID)).Err()
		require.NoError(t, err)
	}

	l, err := client.LRange(context.Background(), pullTestSidekiqQueueKey, 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, l, 3)
	require.Contains(t, l[0], jobIDs[2])
	require.Contains(t, l[1], jobIDs[1])
	require.Contains(t, l[2], jobIDs[0])

	q := NewQueue(client)
	now := time.Now()

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		2,
	).Err()
	require.NoError(t, err)

	l, err = client.LRange(context.Background(), pullTestSidekiqQueueKey, 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, l, 1)
	require.Contains(t, l[0], jobIDs[2])

	l, err = client.LRange(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace), 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, l, 2)
	require.Contains(t, l[0], jobIDs[1])
	require.Contains(t, l[1], jobIDs[0])

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
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

	err := client.LPush(context.Background(), pullTestSidekiqQueueKey, `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	q := NewQueue(client)
	now := time.Now()

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		1,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	err = client.LPush(context.Background(), pullTestSidekiqQueueKey, `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)
	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		1,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestPullDequeueStartRecoveredNotExpired(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	err := client.LPush(context.Background(), pullTestSidekiqQueueKey, `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	q := NewQueue(client)
	now := time.Now()

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		1,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)

	err = client.LPush(context.Background(), pullTestSidekiqQueueKey, `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"456",
		now.Unix()+1,
		10,
		1,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:456", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
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

	err := client.LPush(context.Background(), pullTestSidekiqQueueKey, `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err := client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	q := NewQueue(client)
	now := time.Now()

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		1,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)

	err = client.LPush(context.Background(), pullTestSidekiqQueueKey, `{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":"83b27ea26dd65821239ca6aa","created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"456",
		now.Unix()+30,
		10,
		1,
	).Err()
	require.NoError(t, err)

	count, err = client.Exists(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:123", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	count, err = client.Exists(context.Background(), fmt.Sprintf("%s:456", pullTestNamespace)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
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

	// no error without pullers key
	err := q.(*sidekiqQueue).dequeueStopScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestNamespace,
		"123",
	).Err()
	require.NoError(t, err)

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		1,
	).Err()
	require.NoError(t, err)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)

	// remove existing entries
	err = q.(*sidekiqQueue).dequeueStopScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestNamespace,
		"123",
	).Err()
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 0)
}

func TestPullDequeueHeartbeat(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	q := NewQueue(client)
	now := time.Now()

	// no error without pullers key
	err := q.(*sidekiqQueue).dequeueHeartbeatScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestNamespace,
		"123",
		now.Unix(),
		100,
	).Err()
	require.NoError(t, err)

	err = q.(*sidekiqQueue).dequeueStartScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		pullTestSidekiqQueue,
		pullTestNamespace,
		"123",
		now.Unix(),
		10,
		1,
	).Err()
	require.NoError(t, err)

	z, err := client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+10, z[0].Score)

	// extend expiration
	err = q.(*sidekiqQueue).dequeueHeartbeatScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestNamespace,
		"123",
		now.Unix(),
		100,
	).Err()
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+100, z[0].Score)

	// test invalid entries
	err = q.(*sidekiqQueue).dequeueHeartbeatScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestNamespace,
		"456",
		now.Unix(),
		100,
	).Err()
	require.NoError(t, err)

	z, err = client.ZRangeByScoreWithScores(
		context.Background(),
		pullTestPullersKey,
		&redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "123", z[0].Member)
	require.EqualValues(t, now.Unix()+100, z[0].Score)
}

func TestPullDequeueAck(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	jobIDs := []string{"83b27ea26dd65821239ca6aa", "ebb3186ec09c42642f980a20", "7e59de95478191698aa69b22"}
	for _, jobID := range jobIDs {
		err := client.LPush(context.Background(), pullTestSidekiqQueueKey, fmt.Sprintf(`{"class":"TestWorker","args":[],"retry":3,"queue":"default","backtrace":true,"jid":%q,"created_at":1567788641.0875323,"enqueued_at":1567788642.0879307,"retry_count":2,"error_message":"error: test","error_class":"StandardError","failed_at":1567791043,"retried_at":1567791046}"`, jobID)).Err()
		require.NoError(t, err)
	}

	q := NewQueue(client)

	res, err := q.(*sidekiqQueue).dequeueScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		fmt.Sprintf("queue:%s", pullTestSidekiqQueue),
	).Result()
	require.NoError(t, err)

	jobm := res.([]interface{})
	require.Len(t, jobm, 3)
	require.Contains(t, jobm[0], "7e59de95478191698aa69b22")
	require.Contains(t, jobm[1], "ebb3186ec09c42642f980a20")
	require.Contains(t, jobm[2], "83b27ea26dd65821239ca6aa")

	count, err := client.LLen(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	err = q.(*sidekiqQueue).ackScript.Run(context.Background(), client, []string{pullTestNamespace},
		pullTestSidekiqNamespace,
		fmt.Sprintf("queue:%s", pullTestSidekiqQueue),
	).Err()
	require.NoError(t, err)

	count, err = client.LLen(context.Background(), pullTestSidekiqQueueKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}
