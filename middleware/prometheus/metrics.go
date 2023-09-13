package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/taylorchu/work"
)

var (
	jobExecutionTimeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "work",
			Name:      "job_execution_time_seconds",
			Help:      "Time for a job to finish successfully",
			Buckets:   []float64{1, 10, 60},
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
	jobBusy = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "work",
			Name:      "job_busy",
			Help:      "Total jobs that are running now",
		},
		[]string{"namespace", "queue"},
	)
	jobEnqueuedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "work",
			Name:      "job_enqueued_total",
			Help:      "Total jobs enqueued",
		},
		[]string{"namespace", "queue", "status"},
	)
	jobReady = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "work",
			Name:      "job_ready",
			Help:      "Total jobs that can be executed now",
		},
		[]string{"namespace", "queue"},
	)
	jobScheduled = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "work",
			Name:      "job_scheduled",
			Help:      "Total jobs that can only be executed in future",
		},
		[]string{"namespace", "queue"},
	)
	jobLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "work",
			Name:      "job_latency_seconds",
			Help:      "Processing delay from oldest ready job",
			Buckets:   []float64{1, 10, 60},
		},
		[]string{"namespace", "queue"},
	)
)

// RegisterMetrics adds all metrics to a prometheus registry.
func RegisterMetrics(r prometheus.Registerer) error {
	for _, m := range []prometheus.Collector{
		jobExecutionTimeSeconds,
		jobExecutedTotal,
		jobBusy,
		jobEnqueuedTotal,
		jobReady,
		jobScheduled,
		jobLatencySeconds,
	} {
		err := r.Register(m)
		if err != nil {
			return err
		}
	}
	return nil
}

// HandleFuncMetrics adds prometheus metrics like executed job count.
func HandleFuncMetrics(f work.HandleFunc) work.HandleFunc {
	return func(job *work.Job, opt *work.DequeueOptions) error {
		jobBusy.WithLabelValues(opt.Namespace, opt.QueueID).Inc()
		defer jobBusy.WithLabelValues(opt.Namespace, opt.QueueID).Dec()
		startTime := time.Now()
		err := f(job, opt)
		if err != nil {
			jobExecutedTotal.WithLabelValues(opt.Namespace, opt.QueueID, "failure").Inc()
			return err
		}
		jobExecutedTotal.WithLabelValues(opt.Namespace, opt.QueueID, "success").Inc()
		jobExecutionTimeSeconds.WithLabelValues(opt.Namespace, opt.QueueID).Observe(time.Since(startTime).Seconds())
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

// ExportQueueMetrics adds prometheus metrics from work.QueueMetrics directly.
func ExportQueueMetrics(m *work.QueueMetrics) error {
	jobReady.WithLabelValues(m.Namespace, m.QueueID).Set(float64(m.ReadyTotal))
	jobScheduled.WithLabelValues(m.Namespace, m.QueueID).Set(float64(m.ScheduledTotal))
	jobLatencySeconds.WithLabelValues(m.Namespace, m.QueueID).Observe(m.Latency.Seconds())
	return nil
}

// ExportWorkerMetrics adds prometheus metrics from Worker.
func ExportWorkerMetrics(w *work.Worker) error {
	all, err := w.ExportMetrics()
	if err != nil {
		return err
	}
	for _, m := range all.Queue {
		err := ExportQueueMetrics(m)
		if err != nil {
			return err
		}
	}
	return nil
}

var (
	_ work.HandleMiddleware  = HandleFuncMetrics
	_ work.EnqueueMiddleware = EnqueueFuncMetrics
)
