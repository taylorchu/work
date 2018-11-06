package discard

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/taylorchu/work"
)

// InvalidPayload discards a job if it has decode error.
func InvalidPayload(f work.HandleFunc) work.HandleFunc {
	return func(job *work.Job, opt *work.DequeueOptions) error {
		err := f(job, opt)
		if err != nil {
			cerr := errors.Cause(err)
			if strings.HasPrefix(cerr.Error(), "msgpack:") {
				return work.ErrUnrecoverable
			}
			return err
		}
		return nil
	}
}

var _ work.HandleMiddleware = InvalidPayload
