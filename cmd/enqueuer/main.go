package main

import (
	"flag"
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/taylorchu/work"
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
	defer redisClient.Close()

	queue := work.NewRedisQueue(redisClient)

	job := work.NewJob()
	job.MarshalPayload(flag.Args())
	err = queue.Enqueue(job, &work.EnqueueOptions{
		Namespace: *namespace,
		QueueID:   "cmd_queue",
	})
	if err != nil {
		log.Fatalln(err)
	}
}
