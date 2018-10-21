package work

import (
	"encoding/json"

	"github.com/go-redis/redis"
)

type redisQueue struct {
	Client *redis.Client

	enqueueScript *redis.Script
	dequeueScript *redis.Script
	ackScript     *redis.Script
}

func NewRedisQueue(client *redis.Client) Queue {
	enqueueScript := redis.NewScript(`
	local opt = cjson.decode(ARGV[1])
	local job_id = ARGV[2]
	local job = ARGV[3]
	local job_key = table.concat({opt.prefix, "job", job_id}, ":")
	local queue_key = table.concat({opt.prefix, "queue"}, ":")

	-- update job fields
	redis.call("hset", job_key, "job", job)

	-- enqueue
	redis.call("zadd", queue_key, opt.at, job_key)
	return redis.status_reply("queued")
	`)

	dequeueScript := redis.NewScript(`
	local opt = cjson.decode(ARGV[1])
	local queue_key = table.concat({opt.prefix, "queue"}, ":")

	-- get job
	local jobs = redis.call("zrangebyscore", queue_key, "-inf", opt.at, "LIMIT", 0, 1)
	if table.getn(jobs) == 0 then
		return redis.error_reply("empty")
	end
	local job_id = jobs[1]
	local resp = redis.call("hgetall", job_id)
	local job = {}
	for i = 1,table.getn(resp),2 do
		job[resp[i]] = resp[i+1]
	end

	-- mark it as "processing" by increasing the score
	redis.call("zincrby", queue_key, opt.locked_sec, job_id)

	-- check whether this job is locked
	if job.locked_by ~= nil and job.locked_at ~= nil and job.locked_by ~= opt.locked_by and tonumber(job.locked_at) > opt.locked_at + opt.locked_sec then
		return redis.error_reply("locked")
	end
	redis.call("hmset", job_id, "locked_by", opt.locked_by, "locked_at", opt.locked_at)

	for k, v in pairs(job) do
		print(k, v)
	end
	return job.job
	`)

	ackScript := redis.NewScript(`
	local opt = cjson.decode(ARGV[1])
	local job_id = ARGV[2]
	local job_key = table.concat({opt.prefix, "job", job_id}, ":")
	local queue_key = table.concat({opt.prefix, "queue"}, ":")

	-- delete job fields
	redis.call("del", job_key)

	-- remove job from the queue
	redis.call("zrem", queue_key, job_key)
	return redis.status_reply("ok")
	`)

	return &redisQueue{
		Client:        client,
		enqueueScript: enqueueScript,
		dequeueScript: dequeueScript,
		ackScript:     ackScript,
	}
}

func (q *redisQueue) Enqueue(job *Job, opt *EnqueueOptions) error {
	jobm, err := json.Marshal(job)
	if err != nil {
		return err
	}
	optm, err := json.Marshal(opt)
	if err != nil {
		return err
	}
	return q.enqueueScript.Run(q.Client, nil, optm, job.ID, jobm).Err()
}

func (q *redisQueue) Dequeue(opt *DequeueOptions) (*Job, error) {
	optm, err := json.Marshal(opt)
	if err != nil {
		return nil, err
	}
	s, err := q.dequeueScript.Run(q.Client, nil, optm).String()
	if err != nil {
		return nil, err
	}
	var job Job
	err = json.Unmarshal([]byte(s), &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (q *redisQueue) Ack(job *Job, opt *DequeueOptions) error {
	optm, err := json.Marshal(opt)
	if err != nil {
		return err
	}
	return q.ackScript.Run(q.Client, nil, optm, job.ID).Err()
}
