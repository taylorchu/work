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

func formatQueueNamespace(namespace, queue string) string {
	return fmt.Sprintf("%s:sidekiq-queue-pull:%s", namespace, queue)
}

// Pull moves jobs from sidekiq-compatible queue into work-compatible queue.
func (q *sidekiqQueue) Pull(opt *PullOptions) error {
	err := opt.Validate()
	if err != nil {
		return err
	}
	queueNamespace := formatQueueNamespace(opt.SidekiqNamespace, opt.SidekiqQueue)
	queueID := uuid.NewString()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expireInSec := 10
	refreshInSec := 2
	err = q.dequeueStartScript.Run(ctx, q.client, []string{queueNamespace},
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
		return q.dequeueStopScript.Run(ctx, q.client, []string{queueNamespace},
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
			case <-time.After(time.Duration(refreshInSec) * time.Second):
				err := q.dequeueHeartbeatScript.Run(ctx, q.client, []string{queueNamespace},
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
		res, err := q.dequeueScript.Run(ctx, q.client, []string{queueNamespace},
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
		err = q.ackScript.Run(ctx, q.client, []string{queueNamespace},
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
