package work

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// HandleFunc runs a job.
type HandleFunc func(*Job) error

type handler struct {
	QueueID    string
	HandleFunc HandleFunc
	JobOptions JobOptions
}

// WorkerOptions is used to create a worker.
type WorkerOptions struct {
	Namespace string
	Queue     Queue
}

// Worker runs jobs.
type Worker struct {
	client    *redis.Client
	namespace string
	queue     Queue

	stop       chan struct{}
	wg         sync.WaitGroup
	handlerMap map[string]handler
}

// NewWorker creates a new worker.
func NewWorker(opt *WorkerOptions) *Worker {
	return &Worker{
		namespace:  opt.Namespace,
		queue:      opt.Queue,
		handlerMap: make(map[string]handler),
	}
}

// JobOptions specifies how a job is executed.
type JobOptions struct {
	MaxExecutionTime time.Duration
	IdleWait         time.Duration
	NumGoroutines    int64
}

// options validation error
var (
	ErrMaxExecutionTime = errors.New("work: max execution time should be > 0")
	ErrNumGoroutines    = errors.New("work: number of goroutines should be > 0")
	ErrIdleWait         = errors.New("work: idle wait should be > 0")
)

// Validate validates JobOptions.
func (opt *JobOptions) Validate() error {
	if opt.MaxExecutionTime <= 0 {
		return ErrMaxExecutionTime
	}
	if opt.IdleWait <= 0 {
		return ErrIdleWait
	}
	if opt.NumGoroutines <= 0 {
		return ErrNumGoroutines
	}
	return nil
}

var (
	// ErrQueueNotFound is returned if the queue is not yet
	// defined with Register().
	ErrQueueNotFound = errors.New("work: queue is not found")
)

// Enqueue enqueues a job.
func (w *Worker) Enqueue(queueID string, job *Job) error {
	_, ok := w.handlerMap[queueID]
	if !ok {
		return ErrQueueNotFound
	}
	return w.queue.Enqueue(job, &EnqueueOptions{
		Namespace: w.namespace,
		QueueID:   queueID,
		At:        NewTime(time.Now()),
	})
}

// Register adds handler for a queue.
func (w *Worker) Register(queueID string, h HandleFunc, opt *JobOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	w.handlerMap[queueID] = handler{
		QueueID:    queueID,
		HandleFunc: h,
		JobOptions: *opt,
	}
	return nil
}

// Start starts the worker.
func (w *Worker) Start() {
	stop := make(chan struct{})
	for _, h := range w.handlerMap {
		for i := int64(0); i < h.JobOptions.NumGoroutines; i++ {
			w.wg.Add(1)
			go func(h handler) {
				defer w.wg.Done()

				for {
					select {
					case <-stop:
						return
					default:
						err := func() error {
							job, err := w.queue.Dequeue(&DequeueOptions{
								Namespace:    w.namespace,
								QueueID:      h.QueueID,
								At:           NewTime(time.Now()),
								InvisibleSec: int64(2 * h.JobOptions.MaxExecutionTime / time.Second),
							})
							if err != nil {
								return err
							}
							err = h.HandleFunc(job)
							if err != nil {
								now := time.Now()
								job.Retries++
								job.LastError = err.Error()
								job.UpdatedAt = NewTime(now)
								return w.queue.Enqueue(job, &EnqueueOptions{
									Namespace: w.namespace,
									QueueID:   h.QueueID,
									At:        NewTime(now.Add(2 * h.JobOptions.MaxExecutionTime)),
								})
							}
							return w.queue.Ack(job, &AckOptions{
								Namespace: w.namespace,
								QueueID:   h.QueueID,
							})
						}()
						if err != nil {
							if err == ErrEmptyQueue {
								time.Sleep(h.JobOptions.IdleWait)
							} else {
								log.Println(err)
							}
						}
					}
				}
			}(h)
		}
	}
	w.stop = stop
}

// Stop stops the worker.
func (w *Worker) Stop() {
	close(w.stop)
	w.wg.Wait()
}
