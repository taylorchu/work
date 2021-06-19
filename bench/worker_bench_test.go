package work

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gocraft/work"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
	work2 "github.com/taylorchu/work"
	"github.com/taylorchu/work/redistest"
)

func BenchmarkWorkerRunJob(b *testing.B) {
	client := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		PoolSize:     10,
		MinIdleConns: 10,
	})
	defer client.Close()

	pool := &redigo.Pool{
		MaxActive: 10,
		MaxIdle:   10,
		Dial:      func() (redigo.Conn, error) { return redigo.Dial("tcp", "127.0.0.1:6379") },
	}
	defer pool.Close()

	for k := 1; k <= 1000; k *= 10 {
		b.Run(fmt.Sprintf("work_v1_%d", k), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				require.NoError(b, redistest.Reset(client))

				wp := work.NewWorkerPoolWithOptions(
					struct{}{}, 1, "{ns1}", pool,
					work.WorkerPoolOptions{
						SleepBackoffs: []int64{1000},
					},
				)

				var wg sync.WaitGroup
				wp.Job("test", func(job *work.Job) error {
					wg.Done()
					return nil
				})

				enqueuer := work.NewEnqueuer("{ns1}", pool)
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
				require.NoError(b, redistest.Reset(client))

				queue := work2.NewRedisQueue(client)
				w := work2.NewWorker(&work2.WorkerOptions{
					Namespace: "{ns1}",
					Queue:     queue,
				})
				var wg sync.WaitGroup
				err := w.Register("test",
					func(*work2.Job, *work2.DequeueOptions) error {
						wg.Done()
						return nil
					},
					&work2.JobOptions{
						MaxExecutionTime: time.Minute,
						IdleWait:         time.Second,
						NumGoroutines:    1,
					},
				)
				require.NoError(b, err)

				for i := 0; i < k; i++ {
					job := work2.NewJob()

					err := queue.Enqueue(job, &work2.EnqueueOptions{
						Namespace: "{ns1}",
						QueueID:   "test",
					})
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
