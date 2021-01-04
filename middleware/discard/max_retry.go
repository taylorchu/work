package discard

import (
	"github.com/taylorchu/work"
)

// MaxRetry discards a job if its retry count is over limit.
func MaxRetry(n int64) work.HandleMiddleware {
	return func(f work.HandleFunc) work.HandleFunc {
		return func(job *work.Job, opt *work.DequeueOptions) error {
			err := f(job, opt)
			if job.Retries >= n {
				return work.ErrUnrecoverable
			}
			return err
		}
	}
}
