package work

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
