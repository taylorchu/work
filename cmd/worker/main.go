package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/middleware/discard"
	"github.com/taylorchu/work/middleware/logrus"
)

func main() {
	namespace := flag.String("namespace", "work", "worker namespace")
	redisURL := flag.String("redis", "redis://localhost:6379", "redis url")
	flag.Parse()

	opt, err := redis.ParseURL(*redisURL)
	if err != nil {
		log.Fatalln(err)
	}
	redisClient := redis.NewClient(opt)

	w := work.NewWorker(&work.WorkerOptions{
		Namespace: *namespace,
		Queue:     work.NewRedisQueue(redisClient),
		ErrorFunc: func(err error) {
			log.Println(err)
		},
	})

	jobOpts := &work.JobOptions{
		MaxExecutionTime: time.Minute,
		IdleWait:         4 * time.Second,
		NumGoroutines:    4,
		HandleMiddleware: []work.HandleMiddleware{
			logrus.HandleFuncLogger,
			discard.After(time.Hour),
		},
	}
	w.Register("cmd_queue", func(job *work.Job, opts *work.DequeueOptions) error {
		var cmd []string
		err := job.UnmarshalPayload(&cmd)
		if err != nil {
			return err
		}
		if len(cmd) == 0 {
			return nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), jobOpts.MaxExecutionTime)
		defer cancel()

		fmt.Println(cmd)
		err = exec.CommandContext(ctx, cmd[0], cmd[1:]...).Run()
		if err != nil {
			return err
		}
		return nil
	}, jobOpts)

	w.Start()
	defer w.Stop()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-c
}
