package sidekiq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/taylorchu/work"
)

type sidekiqQueue struct {
	client          redis.UniversalClient
	enqueueScript   *redis.Script
	enqueueInScript *redis.Script
}

// Queue can enqueue to sidekiq-compatible queue.
type Queue interface {
	work.ExternalEnqueuer
	work.ExternalBulkEnqueuer
}

// NewQueue creates a new queue stored in redis with sidekiq-compatible format.
//
// Jobs that will happen in the future are directly placed on sidekiq scheduled queue.
// This assumes that there is another sidekiq instance that is already running, which moves
// scheduled jobs into corresponding queues.
// https://github.com/mperham/sidekiq/blob/e3839682a3d219b8a3708feab607c74241bc06b8/lib/sidekiq/scheduled.rb#L12
func NewQueue(client redis.UniversalClient) Queue {
	// https://github.com/mperham/sidekiq/blob/e3839682a3d219b8a3708feab607c74241bc06b8/lib/sidekiq/client.rb#L190
	enqueueScript := redis.NewScript(`
	local sidekiq_ns = ARGV[1]
	local sidekiq_queue = ARGV[2]

	local queues_key = "queues"
	local queue_key = table.concat({"queue", sidekiq_queue}, ":")
	if sidekiq_ns ~= "" then
		queues_key = table.concat({sidekiq_ns, queues_key}, ":")
		queue_key = table.concat({sidekiq_ns, queue_key}, ":")
	end

	local lpush_args = {}

	for i = 3,table.getn(ARGV) do
		local jobm = ARGV[i]

		-- enqueue
		table.insert(lpush_args, jobm)
	end
	redis.call("sadd", queues_key, sidekiq_queue)
	return redis.call("lpush", queue_key, unpack(lpush_args))
	`)

	enqueueInScript := redis.NewScript(`
	local sidekiq_ns = ARGV[1]
	local schedule_key = "schedule"
	if sidekiq_ns ~= "" then
		schedule_key = table.concat({sidekiq_ns, schedule_key}, ":")
	end

	local zadd_args = {}

	for i = 2,table.getn(ARGV),2 do
		local at = tonumber(ARGV[i])
		local jobm = ARGV[i+1]

		-- enqueue
		table.insert(zadd_args, at)
		table.insert(zadd_args, jobm)
	end
	return redis.call("zadd", schedule_key, unpack(zadd_args))
	`)

	return &sidekiqQueue{
		client:          client,
		enqueueScript:   enqueueScript,
		enqueueInScript: enqueueInScript,
	}
}

// sidekiq queue id validation errors
var (
	ErrInvalidQueueID = errors.New("sidekiq: queue id should have format: SIDEKIQ_QUEUE/SIDEKIQ_CLASS")
)

// ParseQueueID extracts sidekiq queue and class.
func ParseQueueID(s string) (string, string, error) {
	queueClass := strings.SplitN(s, "/", 2)
	switch len(queueClass) {
	case 2:
		return queueClass[0], queueClass[1], nil
	}
	return "", "", ErrInvalidQueueID
}

// FormatQueueID formats sidekiq queue and class.
func FormatQueueID(queue, class string) string {
	return fmt.Sprintf("%s/%s", queue, class)
}

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
