package work

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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

type redisQueue struct {
	client redis.UniversalClient

	enqueueScript *redis.Script
	dequeueScript *redis.Script
	ackScript     *redis.Script
	findScript    *redis.Script
	metricScript  *redis.Script
}

// JobPromoter can update a job's score in the queue to make it immediately
// eligible for dequeuing without re-enqueuing the entire job.
type JobPromoter interface {
	// PromoteJob updates the job's score to time.Now(). Only affects jobs
	// that exist and have scores <= now (won't demote jobs being processed).
	PromoteJob(jobID string, opt *PromoteOptions) error
}

// RedisQueue implements Queue with other additional capabilities
type RedisQueue interface {
	Queue
	BulkEnqueuer
	BulkDequeuer
	BulkJobFinder
	MetricsExporter
	JobPromoter
}

// NewRedisQueue creates a new queue stored in redis.
func NewRedisQueue(client redis.UniversalClient) RedisQueue {
	enqueueScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local queue_key = table.concat({ns, "queue", queue_id}, ":")

	-- Per-job AllowPromotion (passed alongside each job) selects between
	-- two ZADD variants. Guarded entries use ZADD ... gt so a duplicate
	-- enqueue cannot demote an already-deferred deterministic-ID job;
	-- promoted entries use plain ZADD so worker retry backoff can lower
	-- the score below the InvisibleSec mark set by Dequeue.
	local guarded_args = {}
	local promoted_args = {}

	for i = 3,table.getn(ARGV),4 do
		local at = tonumber(ARGV[i])
		local allow_promotion = ARGV[i+1]
		local job_id = ARGV[i+2]
		local jobm = ARGV[i+3]
		local job_key = table.concat({ns, "job", job_id}, ":")

		-- update job fields
		redis.call("hset", job_key, "msgpack", jobm)

		if allow_promotion == "1" then
			table.insert(promoted_args, at)
			table.insert(promoted_args, job_key)
		else
			table.insert(guarded_args, at)
			table.insert(guarded_args, job_key)
		end
	end

	local added = 0
	if table.getn(guarded_args) > 0 then
		added = added + tonumber(redis.call("zadd", queue_key, "gt", unpack(guarded_args)))
	end
	if table.getn(promoted_args) > 0 then
		added = added + tonumber(redis.call("zadd", queue_key, unpack(promoted_args)))
	end
	return added
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

	metricScript := redis.NewScript(`
	local ns = ARGV[1]
	local queue_id = ARGV[2]
	local at = tonumber(ARGV[3])
	local queue_key = table.concat({ns, "queue", queue_id}, ":")

	local ready_total = redis.call("zcount", queue_key, "-inf", at)
	local scheduled_total = redis.call("zcount", queue_key, string.format("(%d", at), "+inf")

	local job_pairs = redis.call("zrangebyscore", queue_key, "-inf", at, "limit", 0, 1, "withscores")
	local first_job_at = 0
	if table.getn(job_pairs) == 2 then
		first_job_at = tonumber(job_pairs[2])
	end

	return cjson.encode({
		ReadyTotal = ready_total,
		ScheduledTotal = scheduled_total,
		FirstJobAt = first_job_at,
	})
	`)

	return &redisQueue{
		client:        client,
		enqueueScript: enqueueScript,
		dequeueScript: dequeueScript,
		ackScript:     ackScript,
		findScript:    findScript,
		metricScript:  metricScript,
	}
}

func (q *redisQueue) Enqueue(job *Job, opt *EnqueueOptions) error {
	return q.BulkEnqueue([]*Job{job}, opt)
}

func (q *redisQueue) BulkEnqueue(jobs []*Job, opt *EnqueueOptions) error {
	for _, batch := range batchSlice(len(jobs)) {
		err := q.bulkEnqueueSmallBatch(jobs[batch[0]:batch[1]], opt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *redisQueue) bulkEnqueueSmallBatch(jobs []*Job, opt *EnqueueOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}
	args := make([]interface{}, 2+4*len(jobs))
	args[0] = opt.Namespace
	args[1] = opt.QueueID
	for i, job := range jobs {
		jobm, err := marshal(job)
		if err != nil {
			return err
		}
		args[2+4*i] = job.EnqueuedAt.Unix()
		if job.AllowPromotion {
			args[2+4*i+1] = "1"
		} else {
			args[2+4*i+1] = "0"
		}
		args[2+4*i+2] = job.ID
		args[2+4*i+3] = jobm
	}
	return q.enqueueScript.Run(context.Background(), q.client, []string{opt.Namespace}, args...).Err()
}

func (q *redisQueue) Dequeue(opt *DequeueOptions) (*Job, error) {
	jobs, err := q.BulkDequeue(1, opt)
	if err != nil {
		return nil, err
	}
	return jobs[0], nil
}

func (q *redisQueue) BulkDequeue(count int64, opt *DequeueOptions) ([]*Job, error) {
	var jobs []*Job
	for _, batch := range batchSlice(int(count)) {
		foundJobs, err := q.bulkDequeueSmallBatch(int64(batch[1]-batch[0]), opt)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, foundJobs...)
	}
	return jobs, nil
}

func (q *redisQueue) bulkDequeueSmallBatch(count int64, opt *DequeueOptions) ([]*Job, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	res, err := q.dequeueScript.Run(context.Background(), q.client, []string{opt.Namespace},
		opt.Namespace,
		opt.QueueID,
		opt.At.Unix(),
		opt.InvisibleSec,
		count,
	).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
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
	for _, batch := range batchSlice(len(jobs)) {
		err := q.bulkAckSmallBatch(jobs[batch[0]:batch[1]], opt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *redisQueue) bulkAckSmallBatch(jobs []*Job, opt *AckOptions) error {
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
	return q.ackScript.Run(context.Background(), q.client, []string{opt.Namespace}, args...).Err()
}

func (q *redisQueue) BulkFind(jobIDs []string, opt *FindOptions) ([]*Job, error) {
	var jobs []*Job
	for _, batch := range batchSlice(len(jobIDs)) {
		foundJobs, err := q.bulkFindSmallBatch(jobIDs[batch[0]:batch[1]], opt)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, foundJobs...)
	}
	return jobs, nil
}

func (q *redisQueue) bulkFindSmallBatch(jobIDs []string, opt *FindOptions) ([]*Job, error) {
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
	res, err := q.findScript.Run(context.Background(), q.client, []string{opt.Namespace}, args...).Result()
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

func (q *redisQueue) PromoteJob(jobID string, opt *PromoteOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}

	queueKey := opt.Namespace + ":queue:" + opt.QueueID
	jobKey := opt.Namespace + ":job:" + jobID

	// Look up the job's AllowPromotion flag so PromoteJob honors the same
	// per-job semantic as Enqueue. With AllowPromotion=false (default),
	// PromoteJob keeps the GT guard and is effectively a no-op for any
	// job whose score sits in the future (either deferred via dedup or
	// in-flight via Dequeue's InvisibleSec mark). With AllowPromotion=true,
	// the caller has asserted that the job's calling pattern is safe to
	// demote — the typical use case is a subqueue handler middleware that
	// promotes the next gated job after the prior handler Acks.
	//
	// If the job is no longer stored (already Ack'd or never enqueued),
	// the XX flag below would prevent (re-)adding it anyway, so a missing
	// hash is treated as a no-op rather than an error.
	jobm, err := q.client.HGet(context.Background(), jobKey, "msgpack").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return err
	}
	var job Job
	if err := unmarshal(strings.NewReader(jobm), &job); err != nil {
		return err
	}

	// XX always: do not resurrect a job whose hash exists but was removed
	// from the queue ZSET (e.g. after an Ack race) — and never add a
	// member that doesn't already exist. GT only when the job did not
	// opt into promotion.
	return q.client.ZAddArgs(
		context.Background(),
		queueKey,
		redis.ZAddArgs{
			XX: true,
			GT: !job.AllowPromotion,
			Members: []redis.Z{{
				Score:  float64(time.Now().Unix()),
				Member: jobKey,
			}},
		},
	).Err()
}

func (q *redisQueue) GetQueueMetrics(opt *QueueMetricsOptions) (*QueueMetrics, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	res, err := q.metricScript.Run(context.Background(), q.client, []string{opt.Namespace},
		opt.Namespace,
		opt.QueueID,
		opt.At.Unix(),
	).Result()
	if err != nil {
		return nil, err
	}
	var m struct {
		ReadyTotal     int64
		ScheduledTotal int64
		FirstJobAt     int64
	}
	err = json.NewDecoder(strings.NewReader(res.(string))).Decode(&m)
	if err != nil {
		return nil, err
	}
	var latency time.Duration
	if m.FirstJobAt > 0 {
		latency = time.Since(time.Unix(m.FirstJobAt, 0))
	}
	return &QueueMetrics{
		Namespace:      opt.Namespace,
		QueueID:        opt.QueueID,
		ReadyTotal:     m.ReadyTotal,
		ScheduledTotal: m.ScheduledTotal,
		Latency:        latency,
	}, nil
}
