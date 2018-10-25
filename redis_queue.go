package work

import (
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis"
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
	local opt = cjson.decode(ARGV[1])
	local job_id = ARGV[2]
	local job = ARGV[3]
	local job_key = table.concat({opt.ns, "job", job_id}, ":")
	local queue_key = table.concat({opt.ns, "queue", opt.queue_id}, ":")

	-- update job fields
	redis.call("hset", job_key, "json", job)

	-- enqueue
	redis.call("zadd", queue_key, opt.at, job_key)
	return redis.status_reply("queued")
	`)

	dequeueScript := redis.NewScript(`
	local opt = cjson.decode(ARGV[1])
	local queue_key = table.concat({opt.ns, "queue", opt.queue_id}, ":")

	-- get job
	local jobs = redis.call("zrangebyscore", queue_key, "-inf", opt.at, "limit", 0, 1)
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
	if job.json == nil then
		redis.call("zrem", queue_key, job_key)
		return nil
	end

	-- mark it as "processing" by increasing the score
	redis.call("zincrby", queue_key, opt.invisible_sec, job_key)

	return job.json
	`)

	ackScript := redis.NewScript(`
	local opt = cjson.decode(ARGV[1])
	local job_id = ARGV[2]
	local job_key = table.concat({opt.ns, "job", job_id}, ":")
	local queue_key = table.concat({opt.ns, "queue", opt.queue_id}, ":")

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
	optm, err := json.Marshal(opt)
	if err != nil {
		return err
	}
	jobm, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return q.enqueueScript.Run(q.client, nil, optm, job.ID, jobm).Err()
}

func (q *redisQueue) Dequeue(opt *DequeueOptions) (*Job, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	optm, err := json.Marshal(opt)
	if err != nil {
		return nil, err
	}
	s, err := q.dequeueScript.Run(q.client, nil, optm).String()
	if err != nil {
		if err.Error() == "empty" {
			return nil, ErrEmptyQueue
		}
		return nil, err
	}
	var job Job
	err = json.Unmarshal([]byte(s), &job)
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
	optm, err := json.Marshal(opt)
	if err != nil {
		return err
	}
	return q.ackScript.Run(q.client, nil, optm, job.ID).Err()
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
