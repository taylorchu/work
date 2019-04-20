# gocraft/work v2

[![GoDoc](https://godoc.org/github.com/taylorchu/work?status.png)](https://godoc.org/github.com/taylorchu/work)
[![Go Report Card](https://goreportcard.com/badge/github.com/taylorchu/work)](https://goreportcard.com/report/github.com/taylorchu/work)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Ftaylorchu%2Fwork.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Ftaylorchu%2Fwork?ref=badge_shield)


This is the in-progress repo for gocraft/work rewrite.

## Improvements

- [x] queue backend abstraction
    - redis is still the default, but the new design allows custom queue implementation.
- [x] simplify the keyspace design of redis queue backend
    - The new design uses 1 redis hash per job, and 1 redis sorted set for queue.
    - [Interesting read](https://kirshatrov.com/2018/07/20/redis-job-queue/)
- [x] modular
    - The core only catches panics, retries on failure, and waits if a queue is empty.
    - All other functionalities are either removed or moved to separate middlewares.
- [x] support binary payload/args with message pack.
- [x] replace built-in UI with prometheus metrics (use grafana if you want dashboard).
- [x] additional optimizations (alloc + bulk queue ops)
    ```go
    BenchmarkWorkerRunJob/work_v1_1-8         	    3000	    515957 ns/op
    BenchmarkWorkerRunJob/work_v2_1-8         	    5000	    284516 ns/op
    BenchmarkWorkerRunJob/work_v1_10-8        	    1000	   2136546 ns/op
    BenchmarkWorkerRunJob/work_v2_10-8        	    5000	    367997 ns/op
    BenchmarkWorkerRunJob/work_v1_100-8       	     100	  18234023 ns/op
    BenchmarkWorkerRunJob/work_v2_100-8       	    1000	   1759186 ns/op
    BenchmarkWorkerRunJob/work_v1_1000-8      	      10	 162110100 ns/op
    BenchmarkWorkerRunJob/work_v2_1000-8      	     100	  12646080 ns/op
    BenchmarkWorkerRunJob/work_v1_10000-8     	       1	1691287122 ns/op
    BenchmarkWorkerRunJob/work_v2_10000-8     	      10	 144923087 ns/op
    BenchmarkWorkerRunJob/work_v1_100000-8    	       1	17515722574 ns/op
    BenchmarkWorkerRunJob/work_v2_100000-8    	       1	1502468637 ns/op
    PASS
    ok  	github.com/taylorchu/work	87.901s
    ```


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Ftaylorchu%2Fwork.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Ftaylorchu%2Fwork?ref=badge_large)