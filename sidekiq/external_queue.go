package sidekiq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/taylorchu/work"
)

func batchSlice(n int) [][]int {
	const size = 1000
	var batches [][]int
	for i := 0; i < n; i += size {
		j := i + size
		if j > n {
			j = n
		}
		batches = append(batches, []int{i, j})
	}
	return batches
}

func (q *sidekiqQueue) ExternalEnqueue(job *work.Job, opt *work.EnqueueOptions) error {
	return q.ExternalBulkEnqueue([]*work.Job{job}, opt)
}

func (q *sidekiqQueue) ExternalBulkEnqueue(jobs []*work.Job, opt *work.EnqueueOptions) error {
	for _, batch := range batchSlice(len(jobs)) {
		err := q.externalBulkEnqueueSmallBatch(jobs[batch[0]:batch[1]], opt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *sidekiqQueue) externalBulkEnqueueSmallBatch(jobs []*work.Job, opt *work.EnqueueOptions) error {
	now := time.Now()
	for _, enq := range []struct {
		in          bool
		enqueueFunc func([]*work.Job, *work.EnqueueOptions) error
	}{
		{
			in:          true,
			enqueueFunc: q.externalBulkEnqueueIn,
		},
		{
			in:          false,
			enqueueFunc: q.externalBulkEnqueue,
		},
	} {
		var matchedJobs []*work.Job
		for _, job := range jobs {
			if enq.in == job.EnqueuedAt.After(now) {
				matchedJobs = append(matchedJobs, job)
			}
		}
		err := enq.enqueueFunc(matchedJobs, opt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *sidekiqQueue) externalBulkEnqueue(jobs []*work.Job, opt *work.EnqueueOptions) error {
	if len(jobs) == 0 {
		return nil
	}
	sqQueue, sqClass, err := ParseQueueID(opt.QueueID)
	if err != nil {
		return err
	}
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
	return q.enqueueScript.Run(context.Background(), q.client, []string{opt.Namespace}, args...).Err()
}

func (q *sidekiqQueue) externalBulkEnqueueIn(jobs []*work.Job, opt *work.EnqueueOptions) error {
	if len(jobs) == 0 {
		return nil
	}
	sqQueue, sqClass, err := ParseQueueID(opt.QueueID)
	if err != nil {
		return err
	}
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
	return q.enqueueInScript.Run(context.Background(), q.client, []string{opt.Namespace}, args...).Err()
}
