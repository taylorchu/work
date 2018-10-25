package work

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// DequeueFunc generates a job.
type DequeueFunc func(*DequeueOptions) (*Job, error)

// DequeueMiddleware modifies DequeueFunc behavior.
type DequeueMiddleware func(DequeueFunc) DequeueFunc

// EnqueueFunc takes in a job for processing.
type EnqueueFunc func(*Job, *EnqueueOptions) error

// EnqueueMiddleware modifies EnqueueFunc behavior.
type EnqueueMiddleware func(EnqueueFunc) EnqueueFunc

// HandleFunc runs a job.
type HandleFunc func(*Job, *DequeueOptions) error

// HandleMiddleware modifies HandleFunc hehavior.
type HandleMiddleware func(HandleFunc) HandleFunc

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

	DequeueMiddleware []DequeueMiddleware
	EnqueueMiddleware []EnqueueMiddleware
	HandleMiddleware  []HandleMiddleware
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

	// ErrUnrecoverable is returned if the error is unrecoverable.
	// The job will be discarded.
	ErrUnrecoverable = errors.New("work: permanent error")

	// ErrUnsupported is returned if it is not implemented.
	ErrUnsupported = errors.New("work: unsupported")
)

// Enqueue enqueues a job.
func (w *Worker) Enqueue(queueID string, job *Job) error {
	h, ok := w.handlerMap[queueID]
	if !ok {
		return ErrQueueNotFound
	}
	enqueue := w.queue.Enqueue
	for _, mw := range h.JobOptions.EnqueueMiddleware {
		enqueue = mw(enqueue)
	}
	return enqueue(job, &EnqueueOptions{
		Namespace: w.namespace,
		QueueID:   queueID,
		At:        job.CreatedAt,
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

				dequeue := w.queue.Dequeue
				for _, mw := range h.JobOptions.DequeueMiddleware {
					dequeue = mw(dequeue)
				}
				// add idleWait middleware
				idleWait := func(f DequeueFunc) DequeueFunc {
					return func(opt *DequeueOptions) (*Job, error) {
						job, err := f(opt)
						if err != nil {
							if err == ErrEmptyQueue {
								time.Sleep(h.JobOptions.IdleWait)
							}
							return nil, err
						}
						return job, nil
					}
				}
				dequeue = idleWait(dequeue)

				handle := h.HandleFunc
				for _, mw := range h.JobOptions.HandleMiddleware {
					handle = mw(handle)
				}
				// add catchPanic middleware
				handle = catchPanic(handle)
				// add retry middleware
				retry := func(f HandleFunc) HandleFunc {
					return func(job *Job, opt *DequeueOptions) error {
						err := f(job, opt)
						if err != nil && err != ErrUnrecoverable {
							now := time.Now()
							job.Retries++
							job.LastError = err.Error()
							job.UpdatedAt = NewTime(now)
							w.queue.Enqueue(job, &EnqueueOptions{
								Namespace: w.namespace,
								QueueID:   h.QueueID,
								At:        NewTime(now.Add(time.Duration(job.Retries) * 2 * h.JobOptions.MaxExecutionTime)),
							})
							return err
						}
						return w.queue.Ack(job, &AckOptions{
							Namespace: w.namespace,
							QueueID:   h.QueueID,
						})
					}
				}
				handle = retry(handle)

				for {
					select {
					case <-stop:
						return
					default:
						func() error {
							opt := &DequeueOptions{
								Namespace:    w.namespace,
								QueueID:      h.QueueID,
								At:           NewTime(time.Now()),
								InvisibleSec: int64(2 * h.JobOptions.MaxExecutionTime / time.Second),
							}
							job, err := dequeue(opt)
							if err != nil {
								return err
							}
							return handle(job, opt)
						}()
					}
				}
			}(h)
		}
	}
	w.stop = stop
}

// ExportMetrics dumps queue stats if the queue implements MetricsExporter.
func (w *Worker) ExportMetrics() (*Metrics, error) {
	exporter, ok := w.queue.(MetricsExporter)
	if !ok {
		return nil, ErrUnsupported
	}
	var (
		queueMetrics []*QueueMetrics
	)
	for _, h := range w.handlerMap {
		m, err := exporter.GetQueueMetrics(&QueueMetricsOptions{
			Namespace: w.namespace,
			QueueID:   h.QueueID,
			At:        NewTime(time.Now()),
		})
		if err != nil {
			return nil, err
		}
		queueMetrics = append(queueMetrics, m)
	}
	return &Metrics{
		Queue: queueMetrics,
	}, nil
}

// Stop stops the worker.
func (w *Worker) Stop() {
	close(w.stop)
	w.wg.Wait()
}

func catchPanic(f HandleFunc) HandleFunc {
	return func(job *Job, opt *DequeueOptions) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errors.New(fmt.Sprint(r))
			}
		}()
		return f(job, opt)
	}
}
