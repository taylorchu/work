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
}

// Concurrency limits running job count from a queue.
func Concurrency(copt *ConcurrencyOptions) work.DequeueMiddleware {
	// The lock has several phases:
	// (-inf, now - invis_sec]: all entries are stale, and should be gc-ed.
	// (now - invis_sec, now): the entries are invalid but kept so that item locked
	// in the previous period cannot be locked in the current period.
	// [now, +inf): the entries are valid.
	//
	// This means that the entry lifetime is 2 * invis_sec.
	script := redis.NewScript(`
	local opt = cjson.decode(ARGV[1])
	local copt = cjson.decode(ARGV[2])
	local lock_key = table.concat({opt.ns, "lock", opt.queue_id}, ":")

	-- refresh expiry
	redis.call("expire", lock_key, 2 * opt.invisible_sec)

	-- remove stale entries
	redis.call("zremrangebyscore", lock_key, "-inf", opt.at - opt.invisible_sec)

	-- for lock fairness
	if redis.call("zscore", lock_key, copt.worker_id) then
		return 0
	end

	if redis.call("zcount", lock_key, opt.at, "+inf") < copt.max then
		redis.call("zadd", lock_key, opt.at + opt.invisible_sec, copt.worker_id)
		return 1
	end
	return 0
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
			underLimit, err := script.Run(copt.Client, nil, optm, coptm).Int64()
			if err != nil {
				return nil, err
			}
			if underLimit == 1 {
				return f(opt)
			}
			return nil, work.ErrEmptyQueue
		}
	}
}
