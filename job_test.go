package work

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJobDelay(t *testing.T) {
	job := NewJob()
	job2 := job.Delay(time.Hour)

	require.EqualValues(t, 0, job2.CreatedAt.Sub(job.CreatedAt))
	require.EqualValues(t, 0, job2.UpdatedAt.Sub(job.UpdatedAt))
	require.Equal(t, time.Hour, job2.EnqueuedAt.Sub(job.EnqueuedAt))
}

func TestJobMarshalDiffType(t *testing.T) {
	type message1 struct {
		Text1   string
		Text2   string
		Padding int64
	}

	type message2 struct {
		Padding int64
		Text1   string
	}

	type message3 struct {
		Padding int64
		Text2   string
	}

	job := NewJob()
	err := job.MarshalPayload(message1{Text1: "hello", Text2: "world", Padding: 3})
	require.NoError(t, err)

	var msg1 message1
	err = job.UnmarshalPayload(&msg1)
	require.NoError(t, err)
	require.Equal(t, "hello", msg1.Text1)
	require.Equal(t, "world", msg1.Text2)
	require.EqualValues(t, 3, msg1.Padding)

	var msg2 message2
	err = job.UnmarshalPayload(&msg2)
	require.NoError(t, err)
	require.Equal(t, "hello", msg2.Text1)
	require.EqualValues(t, 3, msg2.Padding)

	var msg3 message3
	err = job.UnmarshalPayload(&msg3)
	require.NoError(t, err)
	require.Equal(t, "world", msg3.Text2)
	require.EqualValues(t, 3, msg3.Padding)
}

func TestJobMarshalEmbed(t *testing.T) {
	type text struct {
		Text1 string
		Text2 string
	}

	type message1 struct {
		text
		Padding int64
	}

	type message2 struct {
		Padding int64
		Text1   string
	}

	type message3 struct {
		Padding int64
		Text2   string
	}

	type message4 struct {
		Padding2 int64
		text
	}

	job := NewJob()
	err := job.MarshalPayload(message1{text: text{Text1: "hello", Text2: "world"}, Padding: 3})
	require.NoError(t, err)

	var msg2 message2
	err = job.UnmarshalPayload(&msg2)
	require.NoError(t, err)
	require.Equal(t, "hello", msg2.Text1)
	require.EqualValues(t, 3, msg2.Padding)

	var msg3 message3
	err = job.UnmarshalPayload(&msg3)
	require.NoError(t, err)
	require.Equal(t, "world", msg3.Text2)
	require.EqualValues(t, 3, msg3.Padding)

	var msg4 message4
	err = job.UnmarshalPayload(&msg4)
	require.NoError(t, err)
	require.Equal(t, "hello", msg4.Text1)
	require.Equal(t, "world", msg4.Text2)
	require.EqualValues(t, 0, msg4.Padding2)
}
