package sidekiq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/taylorchu/work"
)

type sidekiqQueue struct {
	work.RedisQueue
	client                 redis.UniversalClient
	enqueueScript          *redis.Script
	enqueueInScript        *redis.Script
	dequeueScript          *redis.Script
	ackScript              *redis.Script
	dequeueStartScript     *redis.Script
	dequeueStopScript      *redis.Script
	dequeueHeartbeatScript *redis.Script
	scheduleScript         *redis.Script
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
	// This improves OSS version of sidekiq and will not lose sidekiq jobs.
	dequeueScript := redis.NewScript(`
	local queue_ns = ARGV[1]
	local queue_id = ARGV[2]

	local queue_key = table.concat({queue_ns, queue_id}, ":")
	return redis.call("lindex", queue_key, -1)
	`)

	ackScript := redis.NewScript(`
	local queue_ns = ARGV[1]
	local queue_id = ARGV[2]
	local jobm = ARGV[3]

	local queue_key = table.concat({queue_ns, queue_id}, ":")
	return redis.call("lrem", queue_key, -1, jobm)
	`)

	dequeueStartScript := redis.NewScript(`
	local sidekiq_ns = ARGV[1]
	local sidekiq_queue = ARGV[2]
	local queue_ns = ARGV[3]
	local queue_id = ARGV[4]
	local at = tonumber(ARGV[5])
	local expire_in_sec = tonumber(ARGV[6])

	local queue_key = table.concat({"queue", sidekiq_queue}, ":")
	if sidekiq_ns ~= "" then
		queue_key = table.concat({sidekiq_ns, queue_key}, ":")
	end
	local pullers_key = table.concat({queue_ns, "pullers"}, ":")
	if redis.call("zadd", pullers_key, "nx", at + expire_in_sec, queue_id) == 0 then
		return 0
	end
	local puller_queue_key = table.concat({queue_ns, queue_id}, ":")
	local old_queue_ids = redis.call("zrangebyscore", pullers_key, "-inf", at, "limit", 0, 1)
	for i, old_queue_id in pairs(old_queue_ids) do
		local old_puller_queue_key = table.concat({queue_ns, old_queue_id}, ":")
		redis.call("rename", old_puller_queue_key, puller_queue_key)
		redis.call("zrem", pullers_key, old_queue_id)
		return 1
	end
	redis.call("rename", queue_key, puller_queue_key)
	return 1
	`)

	dequeueStopScript := redis.NewScript(`
	local queue_ns = ARGV[1]
	local queue_id = ARGV[2]

	local puller_queue_key = table.concat({queue_ns, queue_id}, ":")
	local pullers_key = table.concat({queue_ns, "pullers"}, ":")
	if redis.call("llen", puller_queue_key) == 0 then
		redis.call("del", puller_queue_key)
		return redis.call("zrem", pullers_key, queue_id)
	end
	return 0
	`)

	dequeueHeartbeatScript := redis.NewScript(`
	local queue_ns = ARGV[1]
	local queue_id = ARGV[2]
	local at = tonumber(ARGV[3])
	local expire_in_sec = tonumber(ARGV[4])

	local pullers_key = table.concat({queue_ns, "pullers"}, ":")
	return redis.call("zadd", pullers_key, "xx", at + expire_in_sec, queue_id)
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
		RedisQueue:             work.NewRedisQueue(client),
		client:                 client,
		enqueueScript:          enqueueScript,
		enqueueInScript:        enqueueInScript,
		dequeueScript:          dequeueScript,
		ackScript:              ackScript,
		dequeueStartScript:     dequeueStartScript,
		dequeueStopScript:      dequeueStopScript,
		dequeueHeartbeatScript: dequeueHeartbeatScript,
		scheduleScript:         scheduleScript,
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
	// work-compatible namespace
	Namespace string
	// optional work-compatible queue
	// This allows moving jobs to another redis instance. Without this, these jobs are moved
	// within the same sidekiq redis instance.
	Queue work.Queue
	// sidekiq-compatible namespace
	// Only used by https://github.com/resque/redis-namespace. By default, it is empty.
	SidekiqNamespace string
	// sidekiq-compatible queue like `default`.
	SidekiqQueue string
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
	queueNamespace := fmt.Sprintf("%s:sidekiq-queue-pull:%s", opt.SidekiqNamespace, opt.SidekiqQueue)
	queueID := uuid.NewString()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expireInSec := 10
	err = q.dequeueStartScript.Run(ctx, q.client, nil,
		opt.SidekiqNamespace,
		opt.SidekiqQueue,
		queueNamespace,
		queueID,
		time.Now().Unix(),
		expireInSec,
	).Err()
	if err != nil {
		return err
	}
	defer func() error {
		return q.dequeueStopScript.Run(ctx, q.client, nil,
			queueNamespace,
			queueID,
		).Err()
	}()
	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				err := q.dequeueHeartbeatScript.Run(ctx, q.client, nil,
					queueNamespace,
					queueID,
					time.Now().Unix(),
					expireInSec,
				).Err()
				if err != nil {
					return
				}
			}
		}
	}()

	pull := func() error {
		res, err := q.dequeueScript.Run(ctx, q.client, nil,
			queueNamespace,
			queueID,
		).Result()
		if err != nil {
			return err
		}
		var sqJob sidekiqJob
		err = json.NewDecoder(strings.NewReader(res.(string))).Decode(&sqJob)
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
		queue := opt.Queue
		if queue == nil {
			queue = q.RedisQueue
		}
		var found bool
		if finder, ok := queue.(work.BulkJobFinder); ok {
			// best effort to check for duplicates
			jobs, err := finder.BulkFind([]string{job.ID}, &work.FindOptions{
				Namespace: opt.Namespace,
			})
			if err != nil {
				return err
			}
			found = len(jobs) == 1 && jobs[0] != nil
		}
		if !found {
			err := queue.Enqueue(job, &work.EnqueueOptions{
				Namespace: opt.Namespace,
				QueueID:   FormatQueueID(sqJob.Queue, sqJob.Class),
			})
			if err != nil {
				return err
			}
		}
		err = q.ackScript.Run(ctx, q.client, nil,
			queueNamespace,
			queueID,
			res.(string),
		).Err()
		if err != nil {
			return err
		}
		return nil
	}

	for {
		err := pull()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			return err
		}
	}
}
