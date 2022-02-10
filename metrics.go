package work

import "time"

// QueueMetrics contains metrics from a queue.
type QueueMetrics struct {
	Namespace string
	QueueID   string
	// Total number of jobs that can be executed right now.
	ReadyTotal int64
	// Total number of jobs that are scheduled to run in future.
	ScheduledTotal int64
	// Processing delay from oldest ready job
	Latency time.Duration
}

// MetricsExporter can be implemented by Queue to report metrics.
type MetricsExporter interface {
	GetQueueMetrics(*QueueMetricsOptions) (*QueueMetrics, error)
}

// Metrics wraps metrics reported by MetricsExporter.
type Metrics struct {
	Queue []*QueueMetrics
}

// QueueMetricsOptions specifies how to fetch queue metrics.
type QueueMetricsOptions struct {
	Namespace string
	QueueID   string
	At        time.Time
}

// Validate validates QueueMetricsOptions.
func (opt *QueueMetricsOptions) Validate() error {
	if opt.Namespace == "" {
		return ErrEmptyNamespace
	}
	if opt.QueueID == "" {
		return ErrEmptyQueueID
	}
	if opt.At.IsZero() {
		return ErrAt
	}
	return nil
}
