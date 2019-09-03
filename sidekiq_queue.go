package work

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type sidekiqQueue struct {
	*redisQueue

	enqueueScript  *redis.Script
	dequeueScript  *redis.Script
	scheduleScript *redis.Script
}

// See https://github.com/mperham/sidekiq/wiki/Job-Format.
type sidekiqJob struct {
	Class     string      `json:"class"`
	Jid       string      `json:"jid"`
	Args      interface{} `json:"args"`
	CreatedAt int64       `json:"created_at"`
	Queue     string      `json:"queue,omitempty"`
	Retry     bool        `json:"retry,omitempty"`
}

// NewSidekiqQueue creates a new queue stored in redis with sidekiq-compatible format.
//
// This assumes that there is another sidekiq instance that is already running, which moves
// scheduled jobs into corresponding queues.
// https://github.com/mperham/sidekiq/blob/e3839682a3d219b8a3708feab607c74241bc06b8/lib/sidekiq/scheduled.rb#L12
//
// Enqueued jobs are directly placed on the scheduled job queues. Dequeued jobs are
// moved to work-compatible queue as soon as they can run immediately.
func NewSidekiqQueue(client *redis.Client) Queue {
	// https://github.com/mperham/sidekiq/blob/e3839682a3d219b8a3708feab607c74241bc06b8/lib/sidekiq/client.rb#L190
	enqueueScript := redis.NewScript(`
	local ns = ARGV[1]
	local schedule_key = table.concat({ns, "schedule"}, ":")

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

	// https://github.com/mperham/sidekiq/blob/455e9d56f46f0299eaf3b761596207e15f906a39/lib/sidekiq/fetch.rb#L37
	// The OSS version of sidekiq could lose jobs, so it is ok if we lose some too
	// before jobs are moved to work-compatible format.
	dequeueScript := redis.NewScript(`
	local ns = ARGV[1]
	local sidekiq_queue = ARGV[2]

	local ret = {}
	local queue_key = table.concat({ns, "queue", sidekiq_queue}, ":")

	while true do
		local jobm = redis.call("rpop", queue_key)
		if jobm == false then
			break
		end
		table.insert(ret, jobm)
	end
	if table.getn(ret) == 0 then
		return nil
	end
	return ret
	`)

	scheduleScript := redis.NewScript(`
	local ns = ARGV[1]
	local at = tonumber(ARGV[2])

	-- move scheduled jobs
	local schedule_key = table.concat({ns, "schedule"}, ":")
	local queues_key = table.concat({ns, "queues"}, ":")
	local zrem_args = redis.call("zrangebyscore", schedule_key, "-inf", at)
	for i, jobm in pairs(zrem_args) do
		local job = cjson.decode(jobm)
		local queue_key = table.concat({ns, "queue", job.queue}, ":")

		redis.call("sadd", queues_key, job.queue)
		redis.call("lpush", queue_key, jobm)
	end
	if table.getn(zrem_args) > 0 then
		redis.call("zrem", schedule_key, unpack(zrem_args))
	end
	return table.getn(zrem_args)
	`)

	return &sidekiqQueue{
		redisQueue:     NewRedisQueue(client).(*redisQueue),
		enqueueScript:  enqueueScript,
		dequeueScript:  dequeueScript,
		scheduleScript: scheduleScript,
	}
}

func parseSidekiqQueueID(s string) (queue, class string) {
	queueClass := strings.SplitN(s, "/", 2)
	switch len(queueClass) {
	case 2:
		queue, class = queueClass[0], queueClass[1]
	case 1:
		queue, class = "default", queueClass[0]
	}
	return
}

func toSidekiqJob(job *Job, sqQueue, sqClass string) (*sidekiqJob, error) {
	sqJob := sidekiqJob{
		Class:     sqClass,
		Jid:       job.ID,
		CreatedAt: job.EnqueuedAt.Unix(),
		Queue:     sqQueue,
		Retry:     true,
	}
	if len(job.Payload) > 0 {
		err := job.UnmarshalPayload(&sqJob.Args)
		if err != nil {
			return nil, err
		}
	}
	return &sqJob, nil
}

func fromSidekiqJob(sqJob *sidekiqJob) (*Job, error) {
	job := Job{
		ID:         sqJob.Jid,
		CreatedAt:  time.Unix(sqJob.CreatedAt, 0),
		UpdatedAt:  time.Unix(sqJob.CreatedAt, 0),
		EnqueuedAt: time.Unix(sqJob.CreatedAt, 0),
	}
	if sqJob.Args != nil {
		err := job.MarshalPayload(sqJob.Args)
		if err != nil {
			return nil, err
		}
	}
	return &job, nil
}

func (q *sidekiqQueue) Enqueue(job *Job, opt *EnqueueOptions) error {
	return q.BulkEnqueue([]*Job{job}, opt)
}

func (q *sidekiqQueue) BulkEnqueue(jobs []*Job, opt *EnqueueOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}
	sqQueue, sqClass := parseSidekiqQueueID(opt.QueueID)
	args := make([]interface{}, 1+2*len(jobs))
	args[0] = opt.Namespace
	for i, job := range jobs {
		sqJob, err := toSidekiqJob(job, sqQueue, sqClass)
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
	return q.enqueueScript.Run(q.client, nil, args...).Err()
}

func (q *sidekiqQueue) Dequeue(opt *DequeueOptions) (*Job, error) {
	jobs, err := q.BulkDequeue(1, opt)
	if err != nil {
		return nil, err
	}
	return jobs[0], nil
}

func (q *sidekiqQueue) schedule(ns string, at time.Time) error {
	return q.scheduleScript.Run(q.client, nil, ns, at.Unix()).Err()
}

func (q *sidekiqQueue) BulkDequeue(count int64, opt *DequeueOptions) ([]*Job, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	sqQueue, sqClass := parseSidekiqQueueID(opt.QueueID)
	res, err := q.dequeueScript.Run(q.client, nil,
		opt.Namespace,
		sqQueue,
	).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, err
		}
	} else {
		jobm := res.([]interface{})
		jobs := make([]*Job, len(jobm))
		for i, iface := range jobm {
			var sqJob sidekiqJob
			err := json.NewDecoder(strings.NewReader(iface.(string))).Decode(&sqJob)
			if err != nil {
				return nil, err
			}
			job, err := fromSidekiqJob(&sqJob)
			if err != nil {
				return nil, err
			}
			jobs[i] = job
		}
		err = q.redisQueue.BulkEnqueue(jobs, &EnqueueOptions{
			Namespace: opt.Namespace,
			QueueID:   sqClass,
		})
		if err != nil {
			return nil, err
		}
	}
	return q.redisQueue.BulkDequeue(count, &DequeueOptions{
		Namespace:    opt.Namespace,
		QueueID:      sqClass,
		At:           opt.At,
		InvisibleSec: opt.InvisibleSec,
	})
}

func (q *sidekiqQueue) Ack(job *Job, opt *AckOptions) error {
	return q.BulkAck([]*Job{job}, opt)
}

func (q *sidekiqQueue) BulkAck(jobs []*Job, opt *AckOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	_, sqClass := parseSidekiqQueueID(opt.QueueID)
	return q.redisQueue.BulkAck(jobs, &AckOptions{
		Namespace: opt.Namespace,
		QueueID:   sqClass,
	})
}

var (
	_ MetricsExporter = (*sidekiqQueue)(nil)
	_ bulkEnqueuer    = (*sidekiqQueue)(nil)
	_ bulkDequeuer    = (*sidekiqQueue)(nil)
)

func (q *sidekiqQueue) GetQueueMetrics(opt *QueueMetricsOptions) (*QueueMetrics, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	_, sqClass := parseSidekiqQueueID(opt.QueueID)
	return q.redisQueue.GetQueueMetrics(&QueueMetricsOptions{
		Namespace: opt.Namespace,
		QueueID:   sqClass,
		At:        opt.At,
	})
}
