package sidekiq

import (
	"encoding/json"
	"time"

	"github.com/taylorchu/work"
)

func (q *sidekiqQueue) schedule(ns string, at time.Time) error {
	return q.scheduleScript.Run(q.client, nil, ns, at.Unix()).Err()
}

func (q *sidekiqQueue) ExternalEnqueue(job *work.Job, opt *work.EnqueueOptions) error {
	return q.ExternalBulkEnqueue([]*work.Job{job}, opt)
}

func (q *sidekiqQueue) ExternalBulkEnqueue(jobs []*work.Job, opt *work.EnqueueOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}

	now := time.Now()
	readyJobs := make([]*work.Job, 0, len(jobs))
	scheduledJobs := make([]*work.Job, 0, len(jobs))
	for _, job := range jobs {
		if job.EnqueuedAt.After(now) {
			scheduledJobs = append(scheduledJobs, job)
		} else {
			readyJobs = append(readyJobs, job)
		}
	}

	err = q.externalBulkEnqueue(readyJobs, opt)
	if err != nil {
		return err
	}
	err = q.externalBulkEnqueueIn(scheduledJobs, opt)
	if err != nil {
		return err
	}
	return nil
}

func (q *sidekiqQueue) externalBulkEnqueue(jobs []*work.Job, opt *work.EnqueueOptions) error {
	if len(jobs) == 0 {
		return nil
	}
	sqQueue, sqClass := parseQueueID(opt.QueueID)
	args := make([]interface{}, 2+len(jobs))
	args[0] = opt.Namespace
	args[1] = sqQueue
	for i, job := range jobs {
		sqJob, err := newSidekiqJob(job, sqQueue, sqClass)
		if err != nil {
			return err
		}
		err = sqJob.Validate()
		if err != nil {
			return err
		}
		jobm, err := json.Marshal(sqJob)
		if err != nil {
			return err
		}
		args[2+i] = jobm
	}
	return q.enqueueScript.Run(q.client, nil, args...).Err()
}

func (q *sidekiqQueue) externalBulkEnqueueIn(jobs []*work.Job, opt *work.EnqueueOptions) error {
	if len(jobs) == 0 {
		return nil
	}
	sqQueue, sqClass := parseQueueID(opt.QueueID)
	args := make([]interface{}, 1+2*len(jobs))
	args[0] = opt.Namespace
	for i, job := range jobs {
		sqJob, err := newSidekiqJob(job, sqQueue, sqClass)
		if err != nil {
			return err
		}
		err = sqJob.Validate()
		if err != nil {
			return err
		}
		jobm, err := json.Marshal(sqJob)
		if err != nil {
			return err
		}
		args[1+2*i] = job.EnqueuedAt.Unix()
		args[1+2*i+1] = jobm
	}
	return q.enqueueInScript.Run(q.client, nil, args...).Err()
}

var (
	_ work.ExternalEnqueuer     = (*sidekiqQueue)(nil)
	_ work.ExternalBulkEnqueuer = (*sidekiqQueue)(nil)
)
