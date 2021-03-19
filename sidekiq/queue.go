package sidekiq

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/taylorchu/work"
)

type sidekiqQueue struct {
	redisQueue      work.Queue
	client          redis.UniversalClient
	enqueueScript   *redis.Script
	enqueueInScript *redis.Script
	dequeueScript   *redis.Script
	scheduleScript  *redis.Script
}

// See https://github.com/mperham/sidekiq/wiki/Job-Format.
type sidekiqJob struct {
	Class        string          `json:"class"`
	ID           string          `json:"jid"`
	Args         json.RawMessage `json:"args"`
	CreatedAt    float64         `json:"created_at"`
	EnqueuedAt   float64         `json:"enqueued_at,omitempty"`
	Queue        string          `json:"queue,omitempty"`
	Retry        json.RawMessage `json:"retry,omitempty"`
	RetryCount   int64           `json:"retry_count,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	ErrorClass   string          `json:"error_class,omitempty"`
	FailedAt     float64         `json:"failed_at,omitempty"`
	RetriedAt    float64         `json:"retried_at,omitempty"`
}

// sidekiq job validation errors
var (
	ErrJobEmptyClass      = errors.New("sidekiq: empty job class")
	ErrJobMismatchedClass = errors.New("sidekiq: job class does not match queue id")
	ErrJobEmptyID         = errors.New("sidekiq: empty job id")
	ErrJobCreatedAt       = errors.New("sidekiq: job created_at should be > 0")
	ErrJobEnqueuedAt      = errors.New("sidekiq: job enqueued_at should be > 0")
	ErrJobArgs            = errors.New("sidekiq: job args should be an array")
)

func (j *sidekiqJob) Validate() error {
	if j.Class == "" {
		return ErrJobEmptyClass
	}
	if j.ID == "" {
		return ErrJobEmptyID
	}
	if j.CreatedAt <= 0 {
		return ErrJobCreatedAt
	}
	if j.EnqueuedAt <= 0 {
		return ErrJobEnqueuedAt
	}
	if !(bytes.HasPrefix(j.Args, []byte("[")) && bytes.HasSuffix(j.Args, []byte("]"))) {
		return ErrJobArgs
	}
	return nil
}

// NewQueue creates a new queue stored in redis with sidekiq-compatible format.
//
// This assumes that there is another sidekiq instance that is already running, which moves
// scheduled jobs into corresponding queues.
// https://github.com/mperham/sidekiq/blob/e3839682a3d219b8a3708feab607c74241bc06b8/lib/sidekiq/scheduled.rb#L12
//
// Enqueued jobs are directly placed on the scheduled job queues. Dequeued jobs are
// moved to work-compatible queue as soon as they can run immediately.
func NewQueue(client redis.UniversalClient) work.Queue {
	// https://github.com/mperham/sidekiq/blob/e3839682a3d219b8a3708feab607c74241bc06b8/lib/sidekiq/client.rb#L190
	enqueueScript := redis.NewScript(`
	local ns = ARGV[1]
	local sidekiq_queue = ARGV[2]

	local lpush_args = {}
	local queues_key = table.concat({ns, "queues"}, ":")
	local queue_key = table.concat({ns, "queue", sidekiq_queue}, ":")

	for i = 3,table.getn(ARGV) do
		local jobm = ARGV[i]

		-- enqueue
		table.insert(lpush_args, jobm)
	end
	redis.call("sadd", queues_key, sidekiq_queue)
	return redis.call("lpush", queue_key, unpack(lpush_args))
	`)

	enqueueInScript := redis.NewScript(`
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
		redisQueue:      work.NewRedisQueue(client),
		client:          client,
		enqueueScript:   enqueueScript,
		enqueueInScript: enqueueInScript,
		dequeueScript:   dequeueScript,
		scheduleScript:  scheduleScript,
	}
}

func parseQueueID(s string) (queue, class string) {
	queueClass := strings.SplitN(s, "/", 2)
	switch len(queueClass) {
	case 2:
		queue, class = queueClass[0], queueClass[1]
	case 1:
		queue, class = "default", queueClass[0]
	}
	return
}

func newSidekiqJob(job *work.Job, sqQueue, sqClass string) (*sidekiqJob, error) {
	errorClass := ""
	failedAt := int64(0)
	retriedAt := int64(0)
	if job.LastError != "" {
		errorClass = "StandardError"
		failedAt = job.UpdatedAt.Unix()
		retriedAt = job.UpdatedAt.Unix()
	}
	sqJob := sidekiqJob{
		Class:        sqClass,
		ID:           job.ID,
		Args:         job.Payload,
		CreatedAt:    float64(job.CreatedAt.Unix()),
		EnqueuedAt:   float64(job.EnqueuedAt.Unix()),
		Queue:        sqQueue,
		Retry:        []byte("true"),
		RetryCount:   job.Retries,
		ErrorMessage: job.LastError,
		ErrorClass:   errorClass,
		FailedAt:     float64(failedAt),
		RetriedAt:    float64(retriedAt),
	}
	return &sqJob, nil
}

func newJob(sqJob *sidekiqJob) (*work.Job, error) {
	updatedAt := sqJob.CreatedAt
	for _, ts := range []float64{sqJob.FailedAt, sqJob.RetriedAt} {
		if ts > updatedAt {
			updatedAt = ts
		}
	}
	job := work.Job{
		ID:         sqJob.ID,
		Payload:    sqJob.Args,
		CreatedAt:  time.Unix(int64(sqJob.CreatedAt), 0),
		UpdatedAt:  time.Unix(int64(updatedAt), 0),
		EnqueuedAt: time.Unix(int64(sqJob.EnqueuedAt), 0),
		Retries:    sqJob.RetryCount,
		LastError:  sqJob.ErrorMessage,
	}
	return &job, nil
}

func (q *sidekiqQueue) Enqueue(job *work.Job, opt *work.EnqueueOptions) error {
	return q.BulkEnqueue([]*work.Job{job}, opt)
}

func (q *sidekiqQueue) BulkEnqueue(jobs []*work.Job, opt *work.EnqueueOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}

	now := time.Now()
	readyJobs := make([]*work.Job, 0, len(jobs))
	scheduledJobs := make([]*work.Job, 0, len(jobs))
	for _, job := range jobs {
		if job.EnqueuedAt.After(now) {
			scheduledJobs = append(scheduledJobs, job)
		} else {
			readyJobs = append(readyJobs, job)
		}
	}

	err = q.bulkEnqueue(readyJobs, opt)
	if err != nil {
		return err
	}
	err = q.bulkEnqueueIn(scheduledJobs, opt)
	if err != nil {
		return err
	}
	return nil
}

func (q *sidekiqQueue) bulkEnqueue(jobs []*work.Job, opt *work.EnqueueOptions) error {
	if len(jobs) == 0 {
		return nil
	}
	sqQueue, sqClass := parseQueueID(opt.QueueID)
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
	return q.enqueueScript.Run(q.client, nil, args...).Err()
}

func (q *sidekiqQueue) bulkEnqueueIn(jobs []*work.Job, opt *work.EnqueueOptions) error {
	if len(jobs) == 0 {
		return nil
	}
	sqQueue, sqClass := parseQueueID(opt.QueueID)
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
	return q.enqueueInScript.Run(q.client, nil, args...).Err()
}

func (q *sidekiqQueue) Dequeue(opt *work.DequeueOptions) (*work.Job, error) {
	jobs, err := q.BulkDequeue(1, opt)
	if err != nil {
		return nil, err
	}
	return jobs[0], nil
}

func (q *sidekiqQueue) schedule(ns string, at time.Time) error {
	return q.scheduleScript.Run(q.client, nil, ns, at.Unix()).Err()
}

func (q *sidekiqQueue) BulkDequeue(count int64, opt *work.DequeueOptions) ([]*work.Job, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	sqQueue, sqClass := parseQueueID(opt.QueueID)
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
		jobs := make([]*work.Job, len(jobm))
		for i, iface := range jobm {
			var sqJob sidekiqJob
			err := json.NewDecoder(strings.NewReader(iface.(string))).Decode(&sqJob)
			if err != nil {
				return nil, err
			}
			err = sqJob.Validate()
			if err != nil {
				return nil, err
			}
			if sqJob.Class != sqClass {
				return nil, ErrJobMismatchedClass
			}
			job, err := newJob(&sqJob)
			if err != nil {
				return nil, err
			}
			jobs[i] = job
		}
		err = q.redisQueue.(work.BulkEnqueuer).BulkEnqueue(jobs, &work.EnqueueOptions{
			Namespace: opt.Namespace,
			QueueID:   sqClass,
		})
		if err != nil {
			return nil, err
		}
	}
	return q.redisQueue.(work.BulkDequeuer).BulkDequeue(count, &work.DequeueOptions{
		Namespace:    opt.Namespace,
		QueueID:      sqClass,
		At:           opt.At,
		InvisibleSec: opt.InvisibleSec,
	})
}

func (q *sidekiqQueue) Ack(job *work.Job, opt *work.AckOptions) error {
	return q.BulkAck([]*work.Job{job}, opt)
}

func (q *sidekiqQueue) BulkAck(jobs []*work.Job, opt *work.AckOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	_, sqClass := parseQueueID(opt.QueueID)
	return q.redisQueue.(work.BulkDequeuer).BulkAck(jobs, &work.AckOptions{
		Namespace: opt.Namespace,
		QueueID:   sqClass,
	})
}

var (
	_ work.MetricsExporter = (*sidekiqQueue)(nil)
	_ work.BulkEnqueuer    = (*sidekiqQueue)(nil)
	_ work.BulkDequeuer    = (*sidekiqQueue)(nil)
)

func (q *sidekiqQueue) GetQueueMetrics(opt *work.QueueMetricsOptions) (*work.QueueMetrics, error) {
	err := opt.Validate()
	if err != nil {
		return nil, err
	}
	_, sqClass := parseQueueID(opt.QueueID)
	return q.redisQueue.(work.MetricsExporter).GetQueueMetrics(&work.QueueMetricsOptions{
		Namespace: opt.Namespace,
		QueueID:   sqClass,
		At:        opt.At,
	})
}
