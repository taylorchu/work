package concurrent

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
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
func Dequeuer(copt *DequeuerOptions) (work.DequeueMiddleware, work.HandleMiddleware) {
	workerID := copt.workerID
	if workerID == "" {
		workerID = uuid.NewString()
	}
	redisKey := func(namespace, queueID string) string {
		return fmt.Sprintf("%s:lock:%s", namespace, queueID)
	}
	return func(f work.DequeueFunc) work.DequeueFunc {
			return func(opt *work.DequeueOptions) (*work.Job, error) {
				lock := &redislock.Lock{
					Client:       copt.Client,
					Key:          redisKey(opt.Namespace, opt.QueueID),
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
				unlock := false
				defer func() {
					if copt.disableUnlock {
						return
					}
					if !unlock {
						return
					}
					lock.Release()
				}()
				job, err := f(opt)
				if err != nil {
					unlock = true
					return nil, err
				}
				return job, nil
			}
		}, func(f work.HandleFunc) work.HandleFunc {
			return func(job *work.Job, opt *work.DequeueOptions) error {
				lock := &redislock.Lock{
					Client:       copt.Client,
					Key:          redisKey(opt.Namespace, opt.QueueID),
					ID:           workerID,
					At:           opt.At,
					ExpireInSec:  opt.InvisibleSec,
					MaxAcquirers: copt.Max,
				}
				defer func() {
					if copt.disableUnlock {
						return
					}
					lock.Release()
				}()
				return f(job, opt)
			}
		}
}
