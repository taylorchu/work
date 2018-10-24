package unique

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
)

func newRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		PoolSize:     10,
		MinIdleConns: 10,
	})
}

func TestEnqueuer(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())

	enq := Enqueuer(&EnqueuerOptions{
		Client: client,
		UniqueFunc: func(*work.Job, *work.EnqueueOptions) ([]byte, time.Duration) {
			return nil, time.Hour
		},
	})

	opt := &work.EnqueueOptions{
		Namespace: "n1",
		QueueID:   "q1",
	}
	var called int
	h := enq(func(*work.Job, *work.EnqueueOptions) error {
		called++
		return nil
	})
	for i := 0; i < 3; i++ {
		err := h(work.NewJob(), opt)
		require.NoError(t, err)
	}
	require.Equal(t, 1, called)

	for i := 0; i < 3; i++ {
		require.NoError(t, client.Del("n1:unique:q1:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855").Err())
		err := h(work.NewJob(), opt)
		require.NoError(t, err)
		require.Equal(t, i+2, called)
	}
}
