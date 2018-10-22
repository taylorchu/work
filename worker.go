package work

import "time"

type Worker struct {
}

type WorkerOptions struct {
	MaxExecutionTime time.Duration
	MaxRateInSec     int64
	IdleWait         time.Duration
}

type HandleFunc func(*Job) error

func (wp *Worker) Register(queueID string, h HandleFunc, opt *WorkerOptions) {

}

func (wp *Worker) Start() {

}

func (wp *Worker) Stop() {

}
