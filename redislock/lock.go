package redislock

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// Lock supports expiring lock with multiple acquirers.
type Lock struct {
	Client redis.UniversalClient

	Key string
	ID  string

	At           time.Time
	ExpireInSec  int64
	MaxAcquirers int64
}

// Acquire creates the lock if possible.
// If it is acquired, true is returned.
// Call Release to unlock.
func (l *Lock) Acquire() (bool, error) {
	lockScript := redis.NewScript(`
	local lock_key = ARGV[1]
	local lock_id = ARGV[2]
	local at = tonumber(ARGV[3])
	local expire_in_sec = tonumber(ARGV[4])
	local max_acquirers = tonumber(ARGV[5])

	-- refresh expiry
	redis.call("expire", lock_key, expire_in_sec)

	-- remove stale entries
	redis.call("zremrangebyscore", lock_key, "-inf", at)

	if redis.call("zcard", lock_key) < max_acquirers then
		return redis.call("zadd", lock_key, "nx", at + expire_in_sec, lock_id)
	end
	return 0
	`)

	acquired, err := lockScript.Run(context.Background(), l.Client, nil,
		l.Key,
		l.ID,
		l.At.Unix(),
		l.ExpireInSec,
		l.MaxAcquirers,
	).Int64()
	if err != nil {
		return false, err
	}
	return acquired > 0, nil
}

// Release clears the lock.
func (l *Lock) Release() error {
	unlockScript := redis.NewScript(`
	local lock_key = ARGV[1]
	local lock_id = ARGV[2]

	return redis.call("zrem", lock_key, lock_id)
	`)
	err := unlockScript.Run(context.Background(), l.Client, nil,
		l.Key,
		l.ID,
	).Err()
	if err != nil {
		return err
	}
	return nil
}
