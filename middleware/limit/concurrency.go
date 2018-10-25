package limit

import (
	"encoding/json"

	"github.com/go-redis/redis"
	"github.com/taylorchu/work"
)

// ConcurrencyOptions defines how many jobs in the same queue can be running at the same time.
type ConcurrencyOptions struct {
	Client   *redis.Client `json:"-"`
	Max      int64         `json:"max"`
	WorkerID string        `json:"worker_id"`

	disableUnlock bool // for testing
}

// Concurrency limits running job count from a queue.
func Concurrency(copt *ConcurrencyOptions) work.DequeueMiddleware {
	lockScript := redis.NewScript(`
	local opt = cjson.decode(ARGV[1])
	local copt = cjson.decode(ARGV[2])
	local lock_key = table.concat({opt.ns, "lock", opt.queue_id}, ":")

	-- refresh expiry
	redis.call("expire", lock_key, opt.invisible_sec)

	-- remove stale entries
	redis.call("zremrangebyscore", lock_key, "-inf", opt.at)

	if redis.call("zcard", lock_key) < copt.max then
		return redis.call("zadd", lock_key, "nx", opt.at + opt.invisible_sec, copt.worker_id)
	end
	return 0
	`)
	unlockScript := redis.NewScript(`
	local opt = cjson.decode(ARGV[1])
	local copt = cjson.decode(ARGV[2])
	local lock_key = table.concat({opt.ns, "lock", opt.queue_id}, ":")

	return redis.call("zrem", lock_key, copt.worker_id)
	`)
	return func(f work.DequeueFunc) work.DequeueFunc {
		return func(opt *work.DequeueOptions) (*work.Job, error) {
			err := opt.Validate()
			if err != nil {
				return nil, err
			}
			optm, err := json.Marshal(opt)
			if err != nil {
				return nil, err
			}
			coptm, err := json.Marshal(copt)
			if err != nil {
				return nil, err
			}
			acquired, err := lockScript.Run(copt.Client, nil, optm, coptm).Int64()
			if err != nil {
				return nil, err
			}
			if acquired == 0 {
				return nil, work.ErrEmptyQueue
			}
			if !copt.disableUnlock {
				defer unlockScript.Run(copt.Client, nil, optm, coptm)
			}
			return f(opt)
		}
	}
}
