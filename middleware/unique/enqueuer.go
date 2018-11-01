package unique

import (
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/taylorchu/work"
)

// Func defines job uniqueness.
type Func func(*work.Job, *work.EnqueueOptions) ([]byte, time.Duration, error)

// EnqueuerOptions defines job unique key generation.
type EnqueuerOptions struct {
	Client *redis.Client
	// If returned []byte is nil, uniqness check is bypassed.
	// Returned time.Duration controls how long the unique key exists.
	UniqueFunc Func
}

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
			h := sha256.New()
			_, err = h.Write(b)
			if err != nil {
				return err
			}
			key := fmt.Sprintf("%s:unique:%s:%x", opt.Namespace, opt.QueueID, h.Sum(nil))
			notExist, err := eopt.Client.SetNX(key, 1, expireIn).Result()
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
