package concurrent

import (
	"github.com/taylorchu/work"
)

// LocalDequeuerOptions defines how many jobs on the same process can be running at the same time.
type LocalDequeuerOptions struct {
	Max int64

	disableUnlock bool // for testing
}

// LocalDequeuer limits running job count from the same process.
func LocalDequeuer(copt *LocalDequeuerOptions) (work.DequeueMiddleware, work.HandleMiddleware) {
	ch := make(chan struct{}, copt.Max)
	return func(f work.DequeueFunc) work.DequeueFunc {
			return func(opt *work.DequeueOptions) (*work.Job, error) {
				acquired := false
				select {
				case ch <- struct{}{}:
					acquired = true
				default:
				}
				if !acquired {
					return nil, work.ErrEmptyQueue
				}
				unlock := false
				defer func() {
					if copt.disableUnlock {
						return
					}
					if !unlock {
						return
					}
					<-ch
				}()
				job, err := f(opt)
				if err != nil {
					unlock = true
					return nil, err
				}
				return job, nil
			}
		}, func(f work.HandleFunc) work.HandleFunc {
			return func(job *work.Job, opt *work.DequeueOptions) error {
				defer func() {
					if copt.disableUnlock {
						return
					}
					<-ch
				}()
				return f(job, opt)
			}
		}
}
