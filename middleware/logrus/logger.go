package logrus

import (
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taylorchu/work"
)

// HandleFuncLogger logs job execution with logrus structured logger.
func HandleFuncLogger(f work.HandleFunc) work.HandleFunc {
	return func(job *work.Job, opt *work.DequeueOptions) error {
		logger := logrus.WithFields(logrus.Fields{
			"queue":     opt.QueueID,
			"namespace": opt.Namespace,
			"job":       job.ID,
		})
		startTime := time.Now()
		err := f(job, opt)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"last_error": job.LastError,
				"retries":    job.Retries,
				"created_at": job.CreatedAt,
				"updated_at": job.UpdatedAt,
				"retry_at":   job.EnqueuedAt,
			}).WithError(err).Error("Job failed.")
			return err
		}
		logger.WithField("total_time", time.Since(startTime)).Info("Job finished successfully.")
		return nil
	}
}

// EnqueueFuncLogger logs job enqueuing with logrus structured logger.
func EnqueueFuncLogger(f work.EnqueueFunc) work.EnqueueFunc {
	return func(job *work.Job, opt *work.EnqueueOptions) error {
		logger := logrus.WithFields(logrus.Fields{
			"queue":     opt.QueueID,
			"namespace": opt.Namespace,
			"job":       job.ID,
		})
		err := f(job, opt)
		if err != nil {
			logger.WithError(err).Error("Job failed to enqueue.")
			return err
		}
		logger.WithField("enqueued_at", job.EnqueuedAt).Info("Job enqueued.")
		return nil
	}
}

var (
	_ work.HandleMiddleware  = HandleFuncLogger
	_ work.EnqueueMiddleware = EnqueueFuncLogger
)
