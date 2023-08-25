package sidekiq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
	return redis.call("lrange", queue_key, 0, -1)
	`)

	ackScript := redis.NewScript(`
	local queue_ns = ARGV[1]
	local queue_id = ARGV[2]

	local queue_key = table.concat({queue_ns, queue_id}, ":")
	return redis.call("del", queue_key)
	`)

	dequeueStartScript := redis.NewScript(`
	local sidekiq_ns = ARGV[1]
	local sidekiq_queue = ARGV[2]
	local queue_ns = ARGV[3]
	local queue_id = ARGV[4]
	local at = tonumber(ARGV[5])
	local expire_in_sec = tonumber(ARGV[6])
	local max_jobs = tonumber(ARGV[7])

	local pullers_key = table.concat({queue_ns, "pullers"}, ":")
	if redis.call("zadd", pullers_key, "nx", at + expire_in_sec, queue_id) == 0 then
		return 0
	end
	local puller_queue_key = table.concat({queue_ns, queue_id}, ":")
	local old_queue_ids = redis.call("zrangebyscore", pullers_key, "-inf", at, "limit", 0, 1)
	for i, old_queue_id in pairs(old_queue_ids) do
		local old_puller_queue_key = table.concat({queue_ns, old_queue_id}, ":")
		if redis.call("exists", old_puller_queue_key) == 1 then
			redis.call("rename", old_puller_queue_key, puller_queue_key)
		end
		redis.call("zrem", pullers_key, old_queue_id)
		return 1
	end

	local queue_key = table.concat({"queue", sidekiq_queue}, ":")
	if sidekiq_ns ~= "" then
		queue_key = table.concat({sidekiq_ns, queue_key}, ":")
	end

	for i = 1,max_jobs do
		local jobm = redis.call("rpop", queue_key)
		if jobm == false then
			break
		end
		redis.call("lpush", puller_queue_key, jobm)
	end
	return 1
	`)

	dequeueStopScript := redis.NewScript(`
	local queue_ns = ARGV[1]
	local queue_id = ARGV[2]

	local puller_queue_key = table.concat({queue_ns, queue_id}, ":")
	local pullers_key = table.concat({queue_ns, "pullers"}, ":")
	if redis.call("llen", puller_queue_key) == 0 then
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
	return q.scheduleScript.Run(context.Background(), q.client, []string{ns}, ns, at.Unix()).Err()
}
