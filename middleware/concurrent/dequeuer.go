package concurrent

import (
	"fmt"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redislock"
)

// DequeuerOptions defines how many jobs in the same queue can be running at the same time.
type DequeuerOptions struct {
	Client redis.UniversalClient
	Max    int64

	workerID      string // for testing
	disableUnlock bool   // for testing
}

// Dequeuer limits running job count from a queue.
func Dequeuer(copt *DequeuerOptions) work.DequeueMiddleware {
	return func(f work.DequeueFunc) work.DequeueFunc {
		workerID := copt.workerID
		if workerID == "" {
			workerID = uuid.New().String()
		}
		return func(opt *work.DequeueOptions) (*work.Job, error) {
			lock := &redislock.Lock{
				Client:       copt.Client,
				Key:          fmt.Sprintf("%s:lock:%s", opt.Namespace, opt.QueueID),
				ID:           workerID,
				At:           opt.At,
				ExpireInSec:  opt.InvisibleSec,
				MaxAcquirers: copt.Max,
			}
			acquired, err := lock.Acquire()
			if err != nil {
				return nil, err
			}
			if !acquired {
				return nil, work.ErrEmptyQueue
			}
			if !copt.disableUnlock {
				defer lock.Release()
			}
			return f(opt)
		}
	}
}
