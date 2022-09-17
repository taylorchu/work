package throttle

import (
	"errors"
	"sync"
	"time"

	"github.com/taylorchu/work"
)

// PollOptions specifies how long we should wait before we poll the next job.
type PollOptions struct {
	TimeSinceLastPoll       time.Duration
	TimeSinceLastEmptyQueue time.Duration
}

// Poll limits job polling rate.
func Poll(copt *PollOptions) work.DequeueMiddleware {
	var mu sync.Mutex
	var lastPoll time.Time
	var lastEmptyQueue time.Time
	return func(f work.DequeueFunc) work.DequeueFunc {
		return func(opt *work.DequeueOptions) (*work.Job, error) {
			mu.Lock()
			defer mu.Unlock()

			if time.Since(lastPoll) < copt.TimeSinceLastPoll {
				return nil, work.ErrEmptyQueue
			}
			if time.Since(lastEmptyQueue) < copt.TimeSinceLastEmptyQueue {
				return nil, work.ErrEmptyQueue
			}
			lastPoll = time.Now()

			job, err := f(opt)
			if err != nil {
				if errors.Is(err, work.ErrEmptyQueue) {
					lastEmptyQueue = time.Now()
				}
				return nil, err
			}
			return job, nil
		}
	}
}
