package work

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gocraft/work"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

func BenchmarkWorkerRunJob(b *testing.B) {
	client := newRedisClient()
	defer client.Close()

	pool := &redigo.Pool{
		MaxActive: 10,
		MaxIdle:   10,
		Dial:      func() (redigo.Conn, error) { return redigo.Dial("tcp", "127.0.0.1:6379") },
	}
	defer pool.Close()

	for k := 1; k <= 100000; k *= 10 {
		b.Run(fmt.Sprintf("work_v1_%d", k), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				require.NoError(b, client.FlushAll().Err())

				wp := work.NewWorkerPoolWithOptions(
					struct{}{}, 1, "ns1", pool,
					work.WorkerPoolOptions{
						SleepBackoffs: []int64{10, 10, 10, 10, 10},
					},
				)

				var wg sync.WaitGroup
				wp.Job("test", func(job *work.Job) error {
					wg.Done()
					return nil
				})

				enqueuer := work.NewEnqueuer("ns1", pool)
				for i := 0; i < k; i++ {
					_, err := enqueuer.Enqueue("test", nil)
					require.NoError(b, err)

					wg.Add(1)
				}

				b.StartTimer()
				wp.Start()
				wg.Wait()
				wp.Stop()
			}
		})
		b.Run(fmt.Sprintf("work_v2_%d", k), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				require.NoError(b, client.FlushAll().Err())

				w := NewWorker(&WorkerOptions{
					Namespace: "ns1",
					Queue:     NewRedisQueue(client),
				})
				var wg sync.WaitGroup
				err := w.Register("test",
					func(*Job, *DequeueOptions) error {
						wg.Done()
						return nil
					},
					&JobOptions{
						MaxExecutionTime: 60 * time.Second,
						IdleWait:         10 * time.Millisecond,
						NumGoroutines:    1,
					},
				)
				require.NoError(b, err)

				for i := 0; i < k; i++ {
					job := NewJob()

					err := w.Enqueue("test", job)
					require.NoError(b, err)

					wg.Add(1)
				}

				b.StartTimer()
				w.Start()
				wg.Wait()
				w.Stop()
			}
		})
	}
}
