package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/taylorchu/work"
)

// ServerOptions specifies how http server can manage work queues.
type ServerOptions struct {
	Queue work.Queue
}

func (opts *ServerOptions) deleteJob(rw http.ResponseWriter, r *http.Request) {
	queue, ok := opts.Queue.(interface {
		work.Queue
		work.BulkJobFinder
	})
	if !ok {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	namespace := r.URL.Query().Get("namespace")
	queueID := r.URL.Query().Get("queue_id")
	jobID := r.URL.Query().Get("job_id")

	job, err := func() (*work.Job, error) {
		jobs, err := queue.BulkFind([]string{jobID}, &work.FindOptions{
			Namespace: namespace,
		})
		if err != nil {
			return nil, err
		}
		if len(jobs) == 1 && jobs[0] != nil {
			err := queue.Ack(jobs[0], &work.AckOptions{
				Namespace: namespace,
				QueueID:   queueID,
			})
			if err != nil {
				return nil, err
			}
			return jobs[0], nil
		}
		return &work.Job{
			ID: jobID,
		}, nil
	}()
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(rw).Encode(struct {
			Error string `json:"error"`
		}{
			Error: err.Error(),
		})
		return
	}
	json.NewEncoder(rw).Encode(struct {
		Namespace string    `json:"namespace"`
		QueueID   string    `json:"queue_id"`
		Job       *work.Job `json:"job"`
	}{
		Namespace: namespace,
		QueueID:   queueID,
		Job:       job,
	})
}

func (opts *ServerOptions) getJob(rw http.ResponseWriter, r *http.Request) {
	queue, ok := opts.Queue.(interface {
		work.Queue
		work.BulkJobFinder
	})
	if !ok {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	namespace := r.URL.Query().Get("namespace")
	jobID := r.URL.Query().Get("job_id")

	job, err := func() (*work.Job, error) {
		jobs, err := queue.BulkFind([]string{jobID}, &work.FindOptions{
			Namespace: namespace,
		})
		if err != nil {
			return nil, err
		}
		if len(jobs) == 1 && jobs[0] != nil {
			return jobs[0], nil
		}
		return &work.Job{
			ID: jobID,
		}, nil
	}()
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(rw).Encode(struct {
			Error string `json:"error"`
		}{
			Error: err.Error(),
		})
		return
	}
	json.NewEncoder(rw).Encode(struct {
		Namespace string    `json:"namespace"`
		Status    string    `json:"status"`
		Job       *work.Job `json:"job"`
	}{
		Namespace: namespace,
		Status:    jobStatus(job),
		Job:       job,
	})
}

func (opts *ServerOptions) createJob(rw http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	queueID := r.URL.Query().Get("queue_id")

	job, err := func() (*work.Job, error) {
		var enqueueRequest struct {
			ID      string          `json:"id"`
			Payload json.RawMessage `json:"payload"`
			Delay   duration        `json:"delay"`
		}
		err := json.NewDecoder(r.Body).Decode(&enqueueRequest)
		if err != nil {
			return nil, err
		}
		job := work.NewJob().Delay(time.Duration(enqueueRequest.Delay))
		if enqueueRequest.ID != "" {
			job.ID = enqueueRequest.ID
		}
		job.Payload = enqueueRequest.Payload
		if finder, ok := opts.Queue.(work.BulkJobFinder); ok {
			// best effort to check for duplicates
			jobs, err := finder.BulkFind([]string{job.ID}, &work.FindOptions{
				Namespace: namespace,
			})
			if err != nil {
				return nil, err
			}
			if len(jobs) == 1 && jobs[0] != nil {
				return jobs[0], nil
			}
		}
		err = opts.Queue.Enqueue(job, &work.EnqueueOptions{
			Namespace: namespace,
			QueueID:   queueID,
		})
		if err != nil {
			return nil, err
		}
		return job, nil
	}()
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(rw).Encode(struct {
			Error string `json:"error"`
		}{
			Error: err.Error(),
		})
		return
	}
	json.NewEncoder(rw).Encode(struct {
		Namespace string    `json:"namespace"`
		QueueID   string    `json:"queue_id"`
		Job       *work.Job `json:"job"`
	}{
		Namespace: namespace,
		QueueID:   queueID,
		Job:       job,
	})
}

func (opts *ServerOptions) getMetrics(rw http.ResponseWriter, r *http.Request) {
	queue, ok := opts.Queue.(interface {
		work.Queue
		work.MetricsExporter
	})
	if !ok {
		rw.WriteHeader(http.StatusNotFound)
		return
	}
	namespace := r.URL.Query().Get("namespace")
	queueID := r.URL.Query().Get("queue_id")

	metrics, err := queue.GetQueueMetrics(&work.QueueMetricsOptions{
		Namespace: namespace,
		QueueID:   queueID,
		At:        time.Now(),
	})
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(rw).Encode(struct {
			Error string `json:"error"`
		}{
			Error: err.Error(),
		})
		return
	}
	json.NewEncoder(rw).Encode(struct {
		Namespace      string        `json:"namespace"`
		QueueID        string        `json:"queue_id"`
		ReadyTotal     int64         `json:"ready_total"`
		ScheduledTotal int64         `json:"scheduled_total"`
		Total          int64         `json:"total"`
		Latency        time.Duration `json:"latency"`
	}{
		Namespace:      metrics.Namespace,
		QueueID:        metrics.QueueID,
		ReadyTotal:     metrics.ReadyTotal,
		ScheduledTotal: metrics.ScheduledTotal,
		Total:          metrics.ReadyTotal + metrics.ScheduledTotal,
		Latency:        metrics.Latency,
	})
}

// NewServer creates new http server that manages work queues.
func NewServer(opts *ServerOptions) http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/jobs", func(rw http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "DELETE":
			opts.deleteJob(rw, r)
		case "GET":
			opts.getJob(rw, r)
		case "POST":
			opts.createJob(rw, r)
		default:
			rw.WriteHeader(http.StatusNotFound)
		}
	})
	m.HandleFunc("/metrics", func(rw http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			opts.getMetrics(rw, r)
		default:
			rw.WriteHeader(http.StatusNotFound)
		}
	})
	return m
}

func jobStatus(job *work.Job) string {
	if job.EnqueuedAt.IsZero() {
		return "completed"
	}
	if job.EnqueuedAt.After(time.Now()) {
		return "scheduled"
	}
	return "ready"
}
