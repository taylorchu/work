package throttle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
)

func TestPollTimeSinceLastPoll(t *testing.T) {
	m := Poll(&PollOptions{
		TimeSinceLastPoll: time.Second,
	})

	var called int
	h := m(func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return work.NewJob(), nil
	})
	h2 := m(func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return nil, work.ErrEmptyQueue
	})

	job, err := h(&work.DequeueOptions{})
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, 1, called)

	job2, err := h(&work.DequeueOptions{})
	require.Error(t, err)
	require.ErrorIs(t, err, work.ErrEmptyQueue)
	require.Nil(t, job2)
	require.Equal(t, 1, called)

	job3, err := h2(&work.DequeueOptions{})
	require.Error(t, err)
	require.ErrorIs(t, err, work.ErrEmptyQueue)
	require.Nil(t, job3)
	require.Equal(t, 1, called)

	time.Sleep(time.Second)

	job4, err := h(&work.DequeueOptions{})
	require.NoError(t, err)
	require.NotNil(t, job4)
	require.Equal(t, 2, called)
}

func TestPollTimeSinceLastEmptyQueue(t *testing.T) {
	m := Poll(&PollOptions{
		TimeSinceLastEmptyQueue: time.Second,
	})

	var called int
	h := m(func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return work.NewJob(), nil
	})
	h2 := m(func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return nil, work.ErrEmptyQueue
	})

	job, err := h(&work.DequeueOptions{})
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, 1, called)

	job2, err := h(&work.DequeueOptions{})
	require.NoError(t, err)
	require.NotNil(t, job2)
	require.Equal(t, 2, called)

	job3, err := h2(&work.DequeueOptions{})
	require.Error(t, err)
	require.ErrorIs(t, err, work.ErrEmptyQueue)
	require.Nil(t, job3)
	require.Equal(t, 3, called)

	job4, err := h(&work.DequeueOptions{})
	require.Error(t, err)
	require.ErrorIs(t, err, work.ErrEmptyQueue)
	require.Nil(t, job4)
	require.Equal(t, 3, called)

	time.Sleep(time.Second)

	job5, err := h(&work.DequeueOptions{})
	require.NoError(t, err)
	require.NotNil(t, job5)
	require.Equal(t, 4, called)
}
