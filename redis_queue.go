package work

import (
	"fmt"
	"strings"

	"github.com/go-redis/redis"
	"github.com/vmihailenco/msgpack"
)

type redisQueue struct {
	client *redis.Client

	enqueueScript *redis.Script
	dequeueScript *redis.Script
	ackScript     *redis.Script
}

// NewRedisQueue creates a new queue stored in redis.
func NewRedisQueue(client *redis.Client) Queue {
	enqueueScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local at = tonumber(ARGV[3])
	local job_id = ARGV[4]
	local job = ARGV[5]
	local job_key = table.concat({ns, "job", job_id}, ":")
	local queue_key = table.concat({ns, "queue", queue_id}, ":")

	-- update job fields
	redis.call("hset", job_key, "msgpack", job)

	-- enqueue
	redis.call("zadd", queue_key, at, job_key)
	return redis.status_reply("queued")
	`)

	dequeueScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local at = tonumber(ARGV[3])
	local invis_sec = tonumber(ARGV[4])
	local queue_key = table.concat({ns, "queue", queue_id}, ":")

	-- get job
	local jobs = redis.call("zrangebyscore", queue_key, "-inf", at, "limit", 0, 1)
	if table.getn(jobs) == 0 then
		return redis.error_reply("empty")
	end
	local job_key = jobs[1]
	local resp = redis.call("hgetall", job_key)
	local job = {}
	for i = 1,table.getn(resp),2 do
		job[resp[i]] = resp[i+1]
	end

	-- job is deleted unexpectedly
	if job.msgpack == nil then
		redis.call("zrem", queue_key, job_key)
		return nil
	end

	-- mark it as "processing" by increasing the score
	redis.call("zincrby", queue_key, invis_sec, job_key)

	return job.msgpack
	`)

	ackScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local job_id = ARGV[3]
	local job_key = table.concat({ns, "job", job_id}, ":")
	local queue_key = table.concat({ns, "queue", queue_id}, ":")

	-- delete job fields
	redis.call("del", job_key)

	-- remove job from the queue
	redis.call("zrem", queue_key, job_key)
	return redis.status_reply("acked")
	`)

	return &redisQueue{
		client:        client,
		enqueueScript: enqueueScript,
		dequeueScript: dequeueScript,
		ackScript:     ackScript,
	}
}

func (q *redisQueue) Enqueue(job *Job, opt *EnqueueOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	jobm, err := msgpack.Marshal(job)
	if err != nil {
		return err
	}
	return q.enqueueScript.Run(q.client, nil,
		opt.Namespace,
		opt.QueueID,
		opt.At.Unix(),
		job.ID,
		jobm,
	).Err()
}

func (q *redisQueue) Dequeue(opt *DequeueOptions) (*Job, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	s, err := q.dequeueScript.Run(q.client, nil,
		opt.Namespace,
		opt.QueueID,
		opt.At.Unix(),
		opt.InvisibleSec,
	).String()
	if err != nil {
		if err.Error() == "empty" {
			return nil, ErrEmptyQueue
		}
		return nil, err
	}
	var job Job
	err = msgpack.NewDecoder(strings.NewReader(s)).Decode(&job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (q *redisQueue) Ack(job *Job, opt *AckOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	return q.ackScript.Run(q.client, nil,
		opt.Namespace,
		opt.QueueID,
		job.ID,
	).Err()
}

var _ MetricsExporter = (*redisQueue)(nil)

func (q *redisQueue) GetQueueMetrics(opt *QueueMetricsOptions) (*QueueMetrics, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	queueKey := fmt.Sprintf("%s:queue:%s", opt.Namespace, opt.QueueID)
	now := fmt.Sprint(opt.At.Unix())
	readyTotal, err := q.client.ZCount(queueKey, "-inf", now).Result()
	if err != nil {
		return nil, err
	}
	scheduledTotal, err := q.client.ZCount(queueKey, "("+now, "+inf").Result()
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
