package sidekiq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/taylorchu/work"
)

type sidekiqQueue struct {
	work.RedisQueue
	client          redis.UniversalClient
	enqueueScript   *redis.Script
	enqueueInScript *redis.Script
	dequeueScript   *redis.Script
	scheduleScript  *redis.Script
}

// sidekiq queue id validation errors
var (
	ErrInvalidQueueID = errors.New("sidekiq: queue id should have format: SIDEKIQ_QUEUE/SIDEKIQ_CLASS")
)

// Queue extends RedisQueue, and allows job pulling from sidekiq-compatible queue.
type Queue interface {
	work.RedisQueue
	JobPuller
	work.ExternalEnqueuer
	work.ExternalBulkEnqueuer
	schedule(string, time.Time) error
}

// NewQueue creates a new queue stored in redis with sidekiq-compatible format.
//
// This assumes that there is another sidekiq instance that is already running, which moves
// scheduled jobs into corresponding queues.
// https://github.com/mperham/sidekiq/blob/e3839682a3d219b8a3708feab607c74241bc06b8/lib/sidekiq/scheduled.rb#L12
//
// Enqueued jobs are directly placed on the scheduled job queues. Dequeued jobs are
// moved to work-compatible queue as soon as they can run immediately.
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

	// https://github.com/mperham/sidekiq/blob/455e9d56f46f0299eaf3b761596207e15f906a39/lib/sidekiq/fetch.rb#L37
	// The OSS version of sidekiq could lose jobs, so it is ok if we lose some too
	// before jobs are moved to work-compatible format.
	dequeueScript := redis.NewScript(`
	local sidekiq_ns = ARGV[1]
	local sidekiq_queue = ARGV[2]

	local queue_key = table.concat({"queue", sidekiq_queue}, ":")
	if sidekiq_ns ~= "" then
		queue_key = table.concat({sidekiq_ns, queue_key}, ":")
	end

	local ret = {}

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
	local sidekiq_ns = ARGV[1]
	local at = tonumber(ARGV[2])

	-- move scheduled jobs
	local schedule_key = "schedule"
	local queues_key = "queues"
	if sidekiq_ns ~= "" then
		schedule_key = table.concat({sidekiq_ns, schedule_key}, ":")
		queues_key = table.concat({sidekiq_ns, queues_key}, ":")
	end

	local zrem_args = redis.call("zrangebyscore", schedule_key, "-inf", at)
	for i, jobm in pairs(zrem_args) do
		local job = cjson.decode(jobm)
		local queue_key = table.concat({"queue", job.queue}, ":")
		if sidekiq_ns ~= "" then
			queue_key = table.concat({sidekiq_ns, queue_key}, ":")
		end
		redis.call("sadd", queues_key, job.queue)
		redis.call("lpush", queue_key, jobm)
	end
	if table.getn(zrem_args) > 0 then
		redis.call("zrem", schedule_key, unpack(zrem_args))
	end
	return table.getn(zrem_args)
	`)

	return &sidekiqQueue{
		RedisQueue:      work.NewRedisQueue(client),
		client:          client,
		enqueueScript:   enqueueScript,
		enqueueInScript: enqueueInScript,
		dequeueScript:   dequeueScript,
		scheduleScript:  scheduleScript,
	}
}

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

func (q *sidekiqQueue) schedule(ns string, at time.Time) error {
	return q.scheduleScript.Run(context.Background(), q.client, nil, ns, at.Unix()).Err()
}

// JobPuller pulls jobs from sidekiq-compatible queue.
type JobPuller interface {
	Pull(*PullOptions) error
}

// PullOptions specifies how a job is pulled from sidekiq-compatible queue.
type PullOptions struct {
	Namespace string
	// Used by https://github.com/resque/redis-namespace. By default, it is empty.
	SidekiqNamespace string
	SidekiqQueue     string
}

// Validate validates PullOptions.
func (opt *PullOptions) Validate() error {
	if opt.Namespace == "" {
		return work.ErrEmptyNamespace
	}
	if opt.SidekiqQueue == "" {
		return work.ErrEmptyQueueID
	}
	return nil
}

// Pull moves jobs from sidekiq-compatible queue into work-compatible queue.
func (q *sidekiqQueue) Pull(opt *PullOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	res, err := q.dequeueScript.Run(context.Background(), q.client, nil,
		opt.SidekiqNamespace,
		opt.SidekiqQueue,
	).Result()
	if err != nil {
		if err != redis.Nil {
			return err
		}
	} else {
		jobm := res.([]interface{})
		for _, iface := range jobm {
			var sqJob sidekiqJob
			err := json.NewDecoder(strings.NewReader(iface.(string))).Decode(&sqJob)
			if err != nil {
				return err
			}
			err = sqJob.Validate()
			if err != nil {
				return err
			}
			job, err := newJob(&sqJob)
			if err != nil {
				return err
			}
			err = q.RedisQueue.Enqueue(job, &work.EnqueueOptions{
				Namespace: opt.Namespace,
				QueueID:   FormatQueueID(sqJob.Queue, sqJob.Class),
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
