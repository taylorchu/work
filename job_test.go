package work

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJobDelay(t *testing.T) {
	job := NewJob()
	job2 := job.Delay(time.Hour)

	require.Equal(t, time.Hour, job2.CreatedAt.Sub(job.CreatedAt))
	require.Equal(t, time.Hour, job2.UpdatedAt.Sub(job.UpdatedAt))
}

func TestJobMarshal(t *testing.T) {
	job := NewJob()

	type message struct {
		Text string
	}

	want := message{Text: "hello"}
	err := job.MarshalPayload(want)
	require.NoError(t, err)
	var got message
	err = job.UnmarshalPayload(&got)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
