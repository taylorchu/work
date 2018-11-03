package limit

import (
	"github.com/go-redis/redis"
	"github.com/taylorchu/work"
)

// DequeuerOptions defines how many jobs in the same queue can be running at the same time.
type DequeuerOptions struct {
	Client   *redis.Client
	WorkerID string
	Max      int64

	disableUnlock bool // for testing
}

// Dequeuer limits running job count from a queue.
func Dequeuer(copt *DequeuerOptions) work.DequeueMiddleware {
	lockScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local at = tonumber(ARGV[3])
	local invis_sec = tonumber(ARGV[4])
	local worker_id = ARGV[5]
	local max = tonumber(ARGV[6])
	local lock_key = table.concat({ns, "lock", queue_id}, ":")

	-- refresh expiry
	redis.call("expire", lock_key, invis_sec)

	-- remove stale entries
	redis.call("zremrangebyscore", lock_key, "-inf", at)

	if redis.call("zcard", lock_key) < max then
		return redis.call("zadd", lock_key, "nx", at + invis_sec, worker_id)
	end
	return 0
	`)
	unlockScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local worker_id = ARGV[3]
	local lock_key = table.concat({ns, "lock", queue_id}, ":")

	return redis.call("zrem", lock_key, worker_id)
	`)
	return func(f work.DequeueFunc) work.DequeueFunc {
		return func(opt *work.DequeueOptions) (*work.Job, error) {
			err := opt.Validate()
			if err != nil {
				return nil, err
			}
			acquired, err := lockScript.Run(copt.Client, nil,
				opt.Namespace,
				opt.QueueID,
				opt.At.Unix(),
				opt.InvisibleSec,
				copt.WorkerID,
				copt.Max,
			).Int64()
			if err != nil {
				return nil, err
			}
			if acquired == 0 {
				return nil, work.ErrEmptyQueue
			}
			if !copt.disableUnlock {
				defer unlockScript.Run(copt.Client, nil,
					opt.Namespace,
					opt.QueueID,
					copt.WorkerID,
				)
			}
			return f(opt)
		}
	}
}
