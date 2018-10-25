package limit

import (
	"fmt"
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

func TestConcurrency(t *testing.T) {
	client := newRedisClient()
	defer client.Close()
	require.NoError(t, client.FlushAll().Err())

	opt := &work.DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "q1",
		At:           work.NewTime(time.Now()),
		InvisibleSec: 60,
	}
	var called int
	h := func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return work.NewJob(), nil
	}

	for i := 0; i < 3; i++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:   client,
			Max:      2,
			WorkerID: fmt.Sprintf("w%d", i),
		})
		_, err := deq(h)(opt)

		require.NoError(t, err)
	}
	require.Equal(t, 3, called)

	// worker 0, 1 get the lock
	// worker 2 is locked
	for i := 0; i < 3; i++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:        client,
			Max:           2,
			WorkerID:      fmt.Sprintf("w%d", i),
			disableUnlock: true,
		})
		_, err := deq(h)(opt)

		if i <= 1 {
			require.NoError(t, err)
		} else {
			require.Equal(t, work.ErrEmptyQueue, err)
		}
	}
	require.Equal(t, 5, called)

	z, err := client.ZRangeByScoreWithScores("ns1:lock:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 2)
	require.Equal(t, "w0", z[0].Member)
	require.Equal(t, "w1", z[1].Member)
	require.EqualValues(t, opt.At.Unix()+60, z[0].Score)
	require.EqualValues(t, opt.At.Unix()+60, z[1].Score)

	require.NoError(t, client.ZRem("ns1:lock:q1", "w1").Err())
	optLater := *opt
	optLater.At = work.NewTime(opt.At.Add(10 * time.Second))
	// worker 0 is locked already
	for i := 0; i < 3; i++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:        client,
			Max:           2,
			WorkerID:      "w0",
			disableUnlock: true,
		})
		_, err := deq(h)(&optLater)
		require.Equal(t, work.ErrEmptyQueue, err)
	}
	require.Equal(t, 5, called)

	z, err = client.ZRangeByScoreWithScores("ns1:lock:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 1)
	require.Equal(t, "w0", z[0].Member)
	require.EqualValues(t, opt.At.Unix()+60, z[0].Score)

	// lock key expired
	// worker 3, 4 get lock
	optExpired := *opt
	optExpired.At = work.NewTime(opt.At.Add(60 * time.Second))
	for i := 3; i < 6; i++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:        client,
			Max:           2,
			WorkerID:      fmt.Sprintf("w%d", i),
			disableUnlock: true,
		})
		_, err := deq(h)(&optExpired)
		if i < 5 {
			require.NoError(t, err)
		} else {
			require.Equal(t, work.ErrEmptyQueue, err)
		}
	}
	require.Equal(t, 7, called)

	z, err = client.ZRangeByScoreWithScores("ns1:lock:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 2)
	require.Equal(t, "w3", z[0].Member)
	require.Equal(t, "w4", z[1].Member)
	require.EqualValues(t, optExpired.At.Unix()+60, z[0].Score)
	require.EqualValues(t, optExpired.At.Unix()+60, z[1].Score)
}

func BenchmarkConcurrency(b *testing.B) {
	b.StopTimer()

	client := newRedisClient()
	defer client.Close()
	require.NoError(b, client.FlushAll().Err())

	opt := &work.DequeueOptions{
		Namespace:    "ns1",
		QueueID:      "q1",
		At:           work.NewTime(time.Now()),
		InvisibleSec: 60,
	}
	var called int
	h := func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return work.NewJob(), nil
	}

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:   client,
			Max:      2,
			WorkerID: fmt.Sprintf("w%d", n),
		})
		deq(h)(opt)
	}
	b.StopTimer()
	require.Equal(b, b.N, called)
}
