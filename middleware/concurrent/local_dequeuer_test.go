package concurrent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
)

func TestLocalDequeuer(t *testing.T) {
	var called int
	h1 := func(*work.DequeueOptions) (*work.Job, error) {
		called++
		return work.NewJob(), nil
	}
	h2 := func(*work.Job, *work.DequeueOptions) error {
		return nil
	}
	runJob := func(m1 work.DequeueMiddleware, m2 work.HandleMiddleware, opt *work.DequeueOptions) error {
		job, err := m1(h1)(opt)
		if err != nil {
			return err
		}
		err = m2(h2)(job, opt)
		if err != nil {
			return err
		}
		return nil
	}

	opt := &work.DequeueOptions{
		Namespace:    "{ns1}",
		QueueID:      "q1",
		At:           time.Now(),
		InvisibleSec: 60,
	}

	m1, m2 := LocalDequeuer(&LocalDequeuerOptions{
		Max: 2,
	})
	for i := 0; i < 3; i++ {
		err := runJob(m1, m2, opt)
		require.NoError(t, err)
	}
	require.Equal(t, 3, called)

	// worker 0, 1 get the lock
	// worker 2 is locked
	m1, m2 = LocalDequeuer(&LocalDequeuerOptions{
		Max:           2,
		disableUnlock: true,
	})
	for i := 0; i < 3; i++ {
		err := runJob(m1, m2, opt)
		if i <= 1 {
			require.NoError(t, err)
		} else {
			require.Equal(t, work.ErrEmptyQueue, err)
		}
	}
	require.Equal(t, 5, called)
}
