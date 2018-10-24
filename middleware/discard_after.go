package middleware

import (
	"time"

	"github.com/taylorchu/work"
)

// DiscardAfter discards a job if it is already stale.
func DiscardAfter(d time.Duration) work.HandleMiddleware {
	return func(f work.HandleFunc) work.HandleFunc {
		return func(job *work.Job, opt *work.DequeueOptions) error {
			err := f(job, opt)
			if err != nil {
				if time.Now().Sub(job.CreatedAt.Time) > d {
					return work.ErrUnrecoverable
				}
				return err
			}
			return nil
		}
	}
}
