package heartbeat

import (
	"context"
	"time"

	"github.com/taylorchu/work"
)

// HeartbeaterOptions defines Heartbeater periodically refreshes InvisibleSec for long-running jobs.
type HeartbeaterOptions struct {
	// Queue is where the current job is currently in.
	Queue work.Queue
	// After the job is dequeued, no other dequeuer can see this job for a while.
	// InvisibleSec controls how long this period is.
	InvisibleSec int64
	// IntervalSec controls how often job InvisibleSec is refreshed.
	IntervalSec int64
}

// Heartbeater refreshes InvisibleSec for long-running jobs periodically.
// As a result, if the job is lost, it won't take a long time before it is retried.
func Heartbeater(hopts *HeartbeaterOptions) work.HandleMiddleware {
	return func(f work.HandleFunc) work.HandleFunc {
		return func(job *work.Job, opt *work.DequeueOptions) error {
			refresh := func() error {
				now := time.Now()
				copiedJob := *job
				copiedJob.UpdatedAt = now
				copiedJob.EnqueuedAt = now.Add(time.Duration(hopts.InvisibleSec) * time.Second)
				return hopts.Queue.Enqueue(&copiedJob, &work.EnqueueOptions{
					Namespace: opt.Namespace,
					QueueID:   opt.QueueID,
				})
			}

			done := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				defer close(done)

				ticker := time.NewTicker(time.Duration(hopts.IntervalSec) * time.Second)
				defer ticker.Stop()

				refresh()
				for {
					select {
					case <-ticker.C:
						refresh()
					case <-ctx.Done():
						return
					}
				}
			}()

			err := f(job, opt)
			cancel()
			<-done
			return err
		}
	}
}
