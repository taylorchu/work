package unique

import (
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/taylorchu/work"
)

// EnqueuerOptions defines job unique key generation.
type EnqueuerOptions struct {
	Client     *redis.Client
	UniqueFunc func(*work.Job, *work.EnqueueOptions) ([]byte, time.Duration)
}

// Enqueuer uses UniqueFunc to ensure job uniqueness in a period.
func Enqueuer(eopt *EnqueuerOptions) work.EnqueueMiddleware {
	return func(f work.EnqueueFunc) work.EnqueueFunc {
		return func(job *work.Job, opt *work.EnqueueOptions) error {
			b, expireIn := eopt.UniqueFunc(job, opt)
			h := sha256.New()
			_, err := h.Write(b)
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
