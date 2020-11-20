package discard

import (
	"time"

	"github.com/taylorchu/work"
)

// After discards a job if it is already stale.
func After(d time.Duration) work.HandleMiddleware {
	return func(f work.HandleFunc) work.HandleFunc {
		return func(job *work.Job, opt *work.DequeueOptions) error {
			if time.Since(job.CreatedAt) > d {
				return work.ErrUnrecoverable
			}
			err := f(job, opt)
			if time.Since(job.CreatedAt) > d {
				return work.ErrUnrecoverable
			}
			return err
		}
	}
}
