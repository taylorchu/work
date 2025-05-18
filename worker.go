package work

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
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

// ContextHandleFunc runs a job.
type ContextHandleFunc func(context.Context, *Job, *DequeueOptions) error

// HandleMiddleware modifies HandleFunc hehavior.
type HandleMiddleware func(HandleFunc) HandleFunc

// BackoffFunc computes when to retry this job from now.
type BackoffFunc func(*Job, *DequeueOptions) time.Duration

type handler struct {
	QueueID    string
	HandleFunc ContextHandleFunc
	JobOptions JobOptions
}

// WorkerOptions is used to create a worker.
type WorkerOptions struct {
	Namespace string
	Queue     Queue
	ErrorFunc func(error)
}

// Worker runs jobs.
type Worker struct {
	opt WorkerOptions

	cancel     func()
	wg         sync.WaitGroup
	handlerMap map[string]handler
}

// NewWorker creates a new worker.
func NewWorker(opt *WorkerOptions) *Worker {
	return &Worker{
		opt:        *opt,
		handlerMap: make(map[string]handler),
	}
}

// JobOptions specifies how a job is executed.
type JobOptions struct {
	MaxExecutionTime time.Duration
	IdleWait         time.Duration
	NumGoroutines    int64
	Backoff          BackoffFunc

	DequeueMiddleware []DequeueMiddleware
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

// Register adds handler for a queue.
// queueID and namespace should be the same as the one used to enqueue.
func (w *Worker) Register(queueID string, h HandleFunc, opt *JobOptions) error {
	return w.RegisterWithContext(queueID, func(ctx context.Context, job *Job, o *DequeueOptions) error {
		return h(job, o)
	}, opt)
}

// RegisterWithContext adds handler for a queue with context.Context.
// queueID and namespace should be the same as the one used to enqueue.
// The context is created with context.WithTimeout set from MaxExecutionTime.
func (w *Worker) RegisterWithContext(queueID string, h ContextHandleFunc, opt *JobOptions) error {
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

// OnceJobOptions specifies how a job is executed.
type OnceJobOptions struct {
	MaxExecutionTime time.Duration
	Backoff          BackoffFunc

	DequeueMiddleware []DequeueMiddleware
	HandleMiddleware  []HandleMiddleware
}

// Validate validates OnceJobOptions.
func (opt *OnceJobOptions) Validate() error {
	if opt.MaxExecutionTime <= 0 {
		return ErrMaxExecutionTime
	}
	return nil
}

// RunOnce simply runs one job from a queue.
// The context is created with context.WithTimeout set from MaxExecutionTime.
//
// This is used with kubernetes where a pod is created directly to run a job.
func (w *Worker) RunOnce(ctx context.Context, queueID string, h ContextHandleFunc, opt *OnceJobOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}

	queue := w.opt.Queue
	ns := w.opt.Namespace

	dequeue := queue.Dequeue
	for _, mw := range opt.DequeueMiddleware {
		dequeue = mw(dequeue)
	}

	handle := func(job *Job, o *DequeueOptions) error {
		//ctx, cancel := context.WithTimeout(ctx, opt.MaxExecutionTime)
		//defer cancel()
		return h(ctx, job, o)
	}
	for _, mw := range opt.HandleMiddleware {
		handle = mw(handle)
	}
	handle = catchPanic(handle)

	b := opt.Backoff
	if b == nil {
		b = defaultBackoff()
	}
	handle = retry(queue, b)(handle)

	dopt := &DequeueOptions{
		Namespace:    ns,
		QueueID:      queueID,
		At:           time.Now(),
		InvisibleSec: int64(opt.MaxExecutionTime / time.Second),
	}
	job, err := dequeue(dopt)
	if err != nil {
		return err
	}
	err = handle(job, dopt)
	if err != nil {
		return err
	}
	err = queue.Ack(job, &AckOptions{
		Namespace: dopt.Namespace,
		QueueID:   dopt.QueueID,
	})
	if err != nil {
		return err
	}
	return nil
}

// Start starts the worker.
func (w *Worker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	for _, h := range w.handlerMap {
		for i := int64(0); i < h.JobOptions.NumGoroutines; i++ {
			w.wg.Add(1)
			go func(h handler) {
				defer w.wg.Done()

				w.start(ctx, h)
			}(h)
		}
	}
}

func (w *Worker) start(ctx context.Context, h handler) {
	// print errors by default so that problems are noticeable.
	errFunc := func(err error) { fmt.Println(err) }
	if w.opt.ErrorFunc != nil {
		errFunc = w.opt.ErrorFunc
	}

	var dequeueMiddleware []DequeueMiddleware
	dequeueMiddleware = append(dequeueMiddleware, h.JobOptions.DequeueMiddleware...)
	dequeueMiddleware = append(dequeueMiddleware, idleWait(ctx, h.JobOptions.IdleWait))

	var handleMiddleware []HandleMiddleware
	handleMiddleware = append(handleMiddleware, h.JobOptions.HandleMiddleware...)
	handleMiddleware = append(handleMiddleware, catchPanic, wrapHandlerError)

	opt := &OnceJobOptions{
		MaxExecutionTime:  h.JobOptions.MaxExecutionTime,
		Backoff:           h.JobOptions.Backoff,
		DequeueMiddleware: dequeueMiddleware,
		HandleMiddleware:  handleMiddleware,
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := w.RunOnce(ctx, h.QueueID, h.HandleFunc, opt)
			if err != nil {
				var wrappedError *wrappedHandlerError
				if errors.As(err, &wrappedError) {
				} else if errors.Is(err, ErrEmptyQueue) {
				} else {
					errFunc(err)
				}
			}
		}
	}
}

// ExportMetrics dumps queue stats if the queue implements MetricsExporter.
func (w *Worker) ExportMetrics() (*Metrics, error) {
	var queueMetrics []*QueueMetrics
	for _, h := range w.handlerMap {
		queue := w.opt.Queue
		ns := w.opt.Namespace
		exporter, ok := queue.(MetricsExporter)
		if !ok {
			continue
		}
		m, err := exporter.GetQueueMetrics(&QueueMetricsOptions{
			Namespace: ns,
			QueueID:   h.QueueID,
			At:        time.Now(),
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
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

func idleWait(ctx context.Context, d time.Duration) DequeueMiddleware {
	return func(f DequeueFunc) DequeueFunc {
		return func(opt *DequeueOptions) (*Job, error) {
			job, err := f(opt)
			if err != nil {
				if errors.Is(err, ErrEmptyQueue) {
					select {
					case <-time.After(d):
					case <-ctx.Done():
					}
				}
				return nil, err
			}
			return job, nil
		}
	}
}

type wrappedHandlerError struct {
	Err error
}

func (e *wrappedHandlerError) Unwrap() error {
	return e.Err
}

func (e *wrappedHandlerError) Error() string {
	return e.Err.Error()
}

func wrapHandlerError(f HandleFunc) HandleFunc {
	return func(job *Job, opt *DequeueOptions) error {
		err := f(job, opt)
		if err != nil {
			return &wrappedHandlerError{Err: err}
		}
		return nil
	}
}

func catchPanic(f HandleFunc) HandleFunc {
	return func(job *Job, opt *DequeueOptions) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v\n\n%s", r, debug.Stack())
			}
		}()
		return f(job, opt)
	}
}

// retry error
var (
	// ErrDoNotRetry is returned if the job should not be retried;
	// this may be because the job is unrecoverable, or because
	// the handler has already rescheduled it.
	ErrDoNotRetry = errors.New("work: do not retry")

	// ErrUnrecoverable is returned if the error is unrecoverable.
	// The job will be discarded.
	ErrUnrecoverable = fmt.Errorf("work: permanent error: %w", ErrDoNotRetry)
)

// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md
func defaultBackoff() BackoffFunc {
	return func(job *Job, opt *DequeueOptions) time.Duration {
		b := backoff.NewExponentialBackOff()
		b.InitialInterval = time.Second
		b.RandomizationFactor = 0.2
		b.Multiplier = 1.6
		b.MaxInterval = time.Hour
		b.MaxElapsedTime = 0
		b.Reset()

		var next time.Duration
		for i := int64(0); i < job.Retries; i++ {
			next = b.NextBackOff()
		}
		return next
	}
}

func retry(queue Queue, backoff BackoffFunc) HandleMiddleware {
	return func(f HandleFunc) HandleFunc {
		return func(job *Job, opt *DequeueOptions) error {
			err := f(job, opt)
			if err != nil {
				if errors.Is(err, ErrUnrecoverable) {
					return nil // ack
				}
				if errors.Is(err, ErrDoNotRetry) {
					// don't ack and don't reenqueue
					return err
				}
				now := time.Now()
				job.Retries++
				job.LastError = err.Error()
				job.UpdatedAt = now

				job.EnqueuedAt = now.Add(backoff(job, opt))
				queue.Enqueue(job, &EnqueueOptions{
					Namespace: opt.Namespace,
					QueueID:   opt.QueueID,
				})
				return err
			}
			return nil
		}
	}
}
