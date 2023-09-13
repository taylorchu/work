package prometheus

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redistest"
)

func TestHandleFuncMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	err := RegisterMetrics(reg)
	require.NoError(t, err)

	job := work.NewJob()
	opt := &work.DequeueOptions{
		Namespace: "{ns1}",
		QueueID:   "q1",
	}
	h := HandleFuncMetrics(func(*work.Job, *work.DequeueOptions) error {
		return nil
	})

	err = h(job, opt)
	require.NoError(t, err)

	h = HandleFuncMetrics(func(*work.Job, *work.DequeueOptions) error {
		return errors.New("no reason")
	})
	err = h(job, opt)
	require.Error(t, err)

	r := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).
		ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))

	t.Log(r.Body.String())
	for _, m := range []string{
		`work_job_executed_total{`,
		`work_job_execution_time_seconds_bucket{`,
		`work_job_busy{`,
	} {
		require.Contains(t, r.Body.String(), m)
	}
}

func TestEnqueueFuncMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	err := RegisterMetrics(reg)
	require.NoError(t, err)

	job := work.NewJob()
	opt := &work.EnqueueOptions{
		Namespace: "{ns1}",
		QueueID:   "q1",
	}
	h := EnqueueFuncMetrics(func(*work.Job, *work.EnqueueOptions) error {
		return nil
	})

	err = h(job, opt)
	require.NoError(t, err)

	h = EnqueueFuncMetrics(func(*work.Job, *work.EnqueueOptions) error {
		return errors.New("no reason")
	})
	err = h(job, opt)
	require.Error(t, err)

	r := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).
		ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))

	t.Log(r.Body.String())
	for _, m := range []string{
		`work_job_enqueued_total{`,
	} {
		require.Contains(t, r.Body.String(), m)
	}
}

func TestExportWorkerMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	err := RegisterMetrics(reg)
	require.NoError(t, err)

	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))

	w := work.NewWorker(&work.WorkerOptions{
		Namespace: "{ns1}",
		Queue:     work.NewRedisQueue(client),
	})
	err = w.Register("test",
		func(*work.Job, *work.DequeueOptions) error { return nil },
		&work.JobOptions{
			MaxExecutionTime: time.Second,
			IdleWait:         time.Second,
			NumGoroutines:    2,
		},
	)
	require.NoError(t, err)

	err = ExportWorkerMetrics(w)
	require.NoError(t, err)

	r := httptest.NewRecorder()
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).
		ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))

	t.Log(r.Body.String())
	for _, m := range []string{
		`job_ready{`,
		`job_scheduled{`,
		`job_latency_seconds_bucket{`,
	} {
		require.Contains(t, r.Body.String(), m)
	}
}
