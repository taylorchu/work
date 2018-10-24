package middleware

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
)

func TestDiscardAfter(t *testing.T) {
	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace: "n1",
		QueueID:   "q1",
	}
	discardAfter := DiscardAfter(time.Minute)
	h := discardAfter(func(*work.Job, *work.DequeueOptions) error {
		return errors.New("no reason")
	})

	err := h(job, opt)
	require.Error(t, err)
	require.NotEqual(t, work.ErrUnrecoverable, err)

	err = h(job.Delay(-time.Hour), opt)
	require.Equal(t, work.ErrUnrecoverable, err)
}
