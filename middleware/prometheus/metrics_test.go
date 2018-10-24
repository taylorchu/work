package prometheus

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
)

func TestHandleFuncMetrics(t *testing.T) {
	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace: "n1",
		QueueID:   "q1",
	}
	h := HandleFuncMetrics(func(*work.Job, *work.DequeueOptions) error {
		return nil
	})

	err := h(job, opt)
	require.NoError(t, err)

	h = HandleFuncMetrics(func(*work.Job, *work.DequeueOptions) error {
		return errors.New("no reason")
	})
	err = h(job, opt)
	require.Error(t, err)

	r := httptest.NewRecorder()
	prometheus.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))

	for _, m := range []string{
		`work_job_executed_total{`,
		`work_job_execution_time_ms{`,
	} {
		require.Contains(t, r.Body.String(), m)
	}
}

func TestEnqueueFuncMetrics(t *testing.T) {
	job := work.NewJob()
	opt := &work.EnqueueOptions{
		Namespace: "n1",
		QueueID:   "q1",
		At:        job.CreatedAt,
	}
	h := EnqueueFuncMetrics(func(*work.Job, *work.EnqueueOptions) error {
		return nil
	})

	err := h(job, opt)
	require.NoError(t, err)

	h = EnqueueFuncMetrics(func(*work.Job, *work.EnqueueOptions) error {
		return errors.New("no reason")
	})
	err = h(job, opt)
	require.Error(t, err)

	r := httptest.NewRecorder()
	prometheus.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))

	for _, m := range []string{
		`work_job_enqueued_total{`,
	} {
		require.Contains(t, r.Body.String(), m)
	}
}
