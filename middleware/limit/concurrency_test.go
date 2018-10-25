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

	// worker 0, 1 get the lock
	// worker 2 is locked
	for i := 0; i < 3; i++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:   client,
			Max:      2,
			WorkerID: fmt.Sprintf("w%d", i),
		})
		_, err := deq(h)(opt)

		if i <= 1 {
			require.NoError(t, err)
		} else {
			require.Equal(t, work.ErrEmptyQueue, err)
		}
	}
	require.Equal(t, 2, called)

	z, err := client.ZRangeByScore("ns1:lock:q1",
		redis.ZRangeBy{
			Min: fmt.Sprint(opt.At.Unix()),
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 2)
	require.Equal(t, []string{"w0", "w1"}, z)
	z, err = client.ZRangeByScore("ns1:lock:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 2)
	require.Equal(t, []string{"w0", "w1"}, z)

	// worker 0 is locked for fairness
	for i := 0; i < 3; i++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:   client,
			Max:      2,
			WorkerID: "w0",
		})
		_, err := deq(h)(opt)
		require.Equal(t, work.ErrEmptyQueue, err)
	}
	require.Equal(t, 2, called)

	// worker 3, 4, 5 are locked by worker 0, 1
	for i := 3; i < 6; i++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:   client,
			Max:      2,
			WorkerID: fmt.Sprintf("w%d", i),
		})
		_, err := deq(h)(opt)

		require.Equal(t, work.ErrEmptyQueue, err)
	}
	require.Equal(t, 2, called)

	// lock key expired
	// worker 0, 1 are locked for fairness
	// worker 2, 3 can proceed
	// worker 4, 5 are locked
	opt.At = work.NewTime(opt.At.Add(time.Minute + time.Second))
	for i := 0; i < 6; i++ {
		deq := Concurrency(&ConcurrencyOptions{
			Client:   client,
			Max:      2,
			WorkerID: fmt.Sprintf("w%d", i),
		})
		_, err := deq(h)(opt)

		if 2 <= i && i <= 3 {
			require.NoError(t, err)
		} else {
			require.Equal(t, work.ErrEmptyQueue, err)
		}
	}
	require.Equal(t, 4, called)

	z, err = client.ZRangeByScore("ns1:lock:q1",
		redis.ZRangeBy{
			Min: fmt.Sprint(opt.At.Unix()),
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 2)
	require.Equal(t, []string{"w2", "w3"}, z)
	z, err = client.ZRangeByScore("ns1:lock:q1",
		redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
	require.NoError(t, err)
	require.Len(t, z, 4)
	require.Equal(t, []string{"w0", "w1", "w2", "w3"}, z)
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
	require.True(b, called <= 2)
}
