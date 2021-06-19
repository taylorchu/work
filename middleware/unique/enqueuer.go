package unique

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/taylorchu/work"
)

// Func defines job uniqueness.
type Func func(*work.Job, *work.EnqueueOptions) ([]byte, time.Duration, error)

// EnqueuerOptions defines job unique key generation.
type EnqueuerOptions struct {
	Client redis.UniversalClient
	// If returned []byte is nil, uniqness check is bypassed.
	// Returned time.Duration controls how long the unique key exists.
	UniqueFunc Func
}

var (
	// ErrDedupDuration is returned when UniqueFunc returns a non-positive duration.
	// The dedup key will expire too soon in redis.
	ErrDedupDuration = errors.New("work: unique duration should be > 0")
)

// Enqueuer uses UniqueFunc to ensure job uniqueness in a period.
func Enqueuer(eopt *EnqueuerOptions) work.EnqueueMiddleware {
	return func(f work.EnqueueFunc) work.EnqueueFunc {
		return func(job *work.Job, opt *work.EnqueueOptions) error {
			b, expireIn, err := eopt.UniqueFunc(job, opt)
			if err != nil {
				return err
			}
			if b == nil {
				return f(job, opt)
			}
			if expireIn <= 0 {
				return ErrDedupDuration
			}

			h := sha256.New()
			_, err = h.Write(b)
			if err != nil {
				return err
			}
			key := fmt.Sprintf("%s:unique:%s:%x", opt.Namespace, opt.QueueID, h.Sum(nil))
			notExist, err := eopt.Client.SetNX(context.Background(), key, 1, expireIn).Result()
			if err != nil {
				return err
			}
			if notExist {
				return f(job, opt)
			}
			return nil
		}
	}
}
