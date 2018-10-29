package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/taylorchu/work"
)

var (
	jobExecutionTimeMs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "work",
			Name:      "job_execution_time_ms",
			Help:      "Time for a job to finish successfully",
		},
		[]string{"namespace", "queue"},
	)
	jobExecutedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "work",
			Name:      "job_executed_total",
			Help:      "Total jobs executed",
		},
		[]string{"namespace", "queue", "status"},
	)
	jobEnqueuedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "work",
			Name:      "job_enqueued_total",
			Help:      "Total jobs enqueued",
		},
		[]string{"namespace", "queue", "status"},
	)
	jobReadyTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "work",
			Name:      "job_ready_total",
			Help:      "Total jobs that can be executed now",
		},
		[]string{"namespace", "queue"},
	)
	jobScheduledTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "work",
			Name:      "job_scheduled_total",
			Help:      "Total jobs that can only be executed in future",
		},
		[]string{"namespace", "queue"},
	)
)

func init() {
	prometheus.MustRegister(jobExecutionTimeMs)
	prometheus.MustRegister(jobExecutedTotal)

	prometheus.MustRegister(jobEnqueuedTotal)

	prometheus.MustRegister(jobReadyTotal)
	prometheus.MustRegister(jobScheduledTotal)
}

// HandleFuncMetrics adds prometheus metrics like executed job count.
func HandleFuncMetrics(f work.HandleFunc) work.HandleFunc {
	return func(job *work.Job, opt *work.DequeueOptions) error {
		startTime := time.Now()
		err := f(job, opt)
		if err != nil {
			jobExecutedTotal.WithLabelValues(opt.Namespace, opt.QueueID, "failure").Inc()
			return err
		}
		jobExecutedTotal.WithLabelValues(opt.Namespace, opt.QueueID, "success").Inc()
		jobExecutionTimeMs.WithLabelValues(opt.Namespace, opt.QueueID).Set(float64(time.Since(startTime).Nanoseconds()) / 1000000)
		return nil
	}
}

// EnqueueFuncMetrics adds prometheus metrics like enqueued job count.
func EnqueueFuncMetrics(f work.EnqueueFunc) work.EnqueueFunc {
	return func(job *work.Job, opt *work.EnqueueOptions) error {
		err := f(job, opt)
		if err != nil {
			jobEnqueuedTotal.WithLabelValues(opt.Namespace, opt.QueueID, "failure").Inc()
			return err
		}
		jobEnqueuedTotal.WithLabelValues(opt.Namespace, opt.QueueID, "success").Inc()
		return nil
	}
}

// ExportWorkerMetrics adds prometheus metrics from work.Worker.
func ExportWorkerMetrics(w *work.Worker) error {
	all, err := w.ExportMetrics()
	if err != nil {
		return err
	}
	for _, m := range all.Queue {
		jobReadyTotal.WithLabelValues(m.Namespace, m.QueueID).Set(float64(m.ReadyTotal))
		jobScheduledTotal.WithLabelValues(m.Namespace, m.QueueID).Set(float64(m.ScheduledTotal))
	}
	return nil
}

var (
	_ work.HandleMiddleware  = HandleFuncMetrics
	_ work.EnqueueMiddleware = EnqueueFuncMetrics
)