package recovery

import (
	"fmt"
	"runtime"

	"github.com/taylorchu/work"
)

// CatchPanic catches a panic from handler.
// It also adds runtime stack for debugging.
func CatchPanic(f work.HandleFunc) work.HandleFunc {
	return func(job *work.Job, opt *work.DequeueOptions) (err error) {
		defer func() {
			if r := recover(); r != nil {
				const size = 4096
				stack := make([]byte, size)
				stack = stack[:runtime.Stack(stack, false)]

				err = fmt.Errorf("panic: %v\n\n%s", r, stack)
			}
		}()
		return f(job, opt)
	}
}

var _ work.HandleMiddleware = CatchPanic
