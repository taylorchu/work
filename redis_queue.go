package work

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
)

type redisQueue struct {
	client redis.UniversalClient

	enqueueScript *redis.Script
	dequeueScript *redis.Script
	ackScript     *redis.Script
	findScript    *redis.Script
}

type RedisQueue interface {
	Queue
	BulkEnqueuer
	BulkDequeuer
	BulkJobFinder
	MetricsExporter
}

// NewRedisQueue creates a new queue stored in redis.
func NewRedisQueue(client redis.UniversalClient) RedisQueue {
	enqueueScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local queue_key = table.concat({ns, "queue", queue_id}, ":")

	local zadd_args = {}

	for i = 3,table.getn(ARGV),3 do
		local at = tonumber(ARGV[i])
		local job_id = ARGV[i+1]
		local jobm = ARGV[i+2]
		local job_key = table.concat({ns, "job", job_id}, ":")

		-- update job fields
		redis.call("hset", job_key, "msgpack", jobm)

		-- enqueue
		table.insert(zadd_args, at)
		table.insert(zadd_args, job_key)
	end
	return redis.call("zadd", queue_key, unpack(zadd_args))
	`)

	dequeueScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local at = tonumber(ARGV[3])
	local invis_sec = tonumber(ARGV[4])
	local count = tonumber(ARGV[5])
	local queue_key = table.concat({ns, "queue", queue_id}, ":")

	-- get job
	local job_keys = redis.call("zrangebyscore", queue_key, "-inf", at, "limit", 0, count)

	local ret = {}

	local zadd_args = {}
	local zrem_args = {}

	for i, job_key in pairs(job_keys) do
		local jobm = redis.call("hget", job_key, "msgpack")

		-- job is deleted unexpectedly
		if jobm == false then
			table.insert(zrem_args, job_key)
		else
			if invis_sec > 0 then
				-- mark it as "processing" by increasing the score
				table.insert(zadd_args, at + invis_sec)
				table.insert(zadd_args, job_key)
			end
			table.insert(ret, jobm)
		end
	end
	if table.getn(zadd_args) > 0 then
		redis.call("zadd", queue_key, "XX", unpack(zadd_args))
	end
	if table.getn(zrem_args) > 0 then
		redis.call("zrem", queue_key, unpack(zrem_args))
	end

	if table.getn(ret) == 0 then
		return nil
	end
	return ret
	`)

	ackScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local queue_key = table.concat({ns, "queue", queue_id}, ":")

	local zrem_args = {}
	local del_args = {}

	for i = 3,table.getn(ARGV) do
		local job_id = ARGV[i]
		local job_key = table.concat({ns, "job", job_id}, ":")

		-- delete job fields
		table.insert(del_args, job_key)

		-- remove job from the queue
		table.insert(zrem_args, job_key)
	end
	redis.call("del", unpack(del_args))
	return redis.call("zrem", queue_key, unpack(zrem_args))
	`)

	findScript := redis.NewScript(`
	local ns = ARGV[1]
	local ret = {}
	for i = 2,table.getn(ARGV) do
		local job_id = ARGV[i]
		local job_key = table.concat({ns, "job", job_id}, ":")
		local jobm = redis.call("hget", job_key, "msgpack")

		table.insert(ret, jobm)
	end
	return ret
	`)

	return &redisQueue{
		client:        client,
		enqueueScript: enqueueScript,
		dequeueScript: dequeueScript,
		ackScript:     ackScript,
		findScript:    findScript,
	}
}

func (q *redisQueue) Enqueue(job *Job, opt *EnqueueOptions) error {
	return q.BulkEnqueue([]*Job{job}, opt)
}

func (q *redisQueue) BulkEnqueue(jobs []*Job, opt *EnqueueOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}
	args := make([]interface{}, 2+3*len(jobs))
	args[0] = opt.Namespace
	args[1] = opt.QueueID
	for i, job := range jobs {
		jobm, err := marshal(job)
		if err != nil {
			return err
		}
		args[2+3*i] = job.EnqueuedAt.Unix()
		args[2+3*i+1] = job.ID
		args[2+3*i+2] = jobm
	}
	return q.enqueueScript.Run(context.Background(), q.client, nil, args...).Err()
}

func (q *redisQueue) Dequeue(opt *DequeueOptions) (*Job, error) {
	jobs, err := q.BulkDequeue(1, opt)
	if err != nil {
		return nil, err
	}
	return jobs[0], nil
}

func (q *redisQueue) BulkDequeue(count int64, opt *DequeueOptions) ([]*Job, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	res, err := q.dequeueScript.Run(context.Background(), q.client, nil,
		opt.Namespace,
		opt.QueueID,
		opt.At.Unix(),
		opt.InvisibleSec,
		count,
	).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrEmptyQueue
		}
		return nil, err
	}
	jobm := res.([]interface{})
	jobs := make([]*Job, len(jobm))
	for i, iface := range jobm {
		var job Job
		err := unmarshal(strings.NewReader(iface.(string)), &job)
		if err != nil {
			return nil, err
		}
		jobs[i] = &job
	}
	return jobs, nil
}

func (q *redisQueue) Ack(job *Job, opt *AckOptions) error {
	return q.BulkAck([]*Job{job}, opt)
}

func (q *redisQueue) BulkAck(jobs []*Job, opt *AckOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}
	args := make([]interface{}, 2+len(jobs))
	args[0] = opt.Namespace
	args[1] = opt.QueueID
	for i, job := range jobs {
		args[2+i] = job.ID
	}
	return q.ackScript.Run(context.Background(), q.client, nil, args...).Err()
}

func (q *redisQueue) BulkFind(jobIDs []string, opt *FindOptions) ([]*Job, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	if len(jobIDs) == 0 {
		return nil, nil
	}
	args := make([]interface{}, 1+len(jobIDs))
	args[0] = opt.Namespace
	for i, jobID := range jobIDs {
		args[1+i] = jobID
	}
	res, err := q.findScript.Run(context.Background(), q.client, nil, args...).Result()
	if err != nil {
		return nil, err
	}
	jobm := res.([]interface{})
	jobs := make([]*Job, len(jobm))
	for i, iface := range jobm {
		switch payload := iface.(type) {
		case string:
			var job Job
			err := unmarshal(strings.NewReader(payload), &job)
			if err != nil {
				return nil, err
			}
			jobs[i] = &job
		}
	}
	return jobs, nil
}

func (q *redisQueue) GetQueueMetrics(opt *QueueMetricsOptions) (*QueueMetrics, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	queueKey := fmt.Sprintf("%s:queue:%s", opt.Namespace, opt.QueueID)
	now := fmt.Sprint(opt.At.Unix())
	readyTotal, err := q.client.ZCount(context.Background(), queueKey, "-inf", now).Result()
	if err != nil {
		return nil, err
	}
	scheduledTotal, err := q.client.ZCount(context.Background(), queueKey, "("+now, "+inf").Result()
	if err != nil {
		return nil, err
	}
	return &QueueMetrics{
		Namespace:      opt.Namespace,
		QueueID:        opt.QueueID,
		ReadyTotal:     readyTotal,
		ScheduledTotal: scheduledTotal,
	}, nil
}
