package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
)

func TestCatchPanic(t *testing.T) {
	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace: "{ns1}",
		QueueID:   "q1",
	}
	h := CatchPanic(func(*work.Job, *work.DequeueOptions) error {
		panic("fatal error")
	})

	err := h(job, opt)
	require.Error(t, err)
}
