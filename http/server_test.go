package http

import (
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/taylorchu/work"
	"github.com/taylorchu/work/redistest"
)

func TestServer(t *testing.T) {
	client := redistest.NewClient()
	defer client.Close()
	require.NoError(t, redistest.Reset(client))
	q := work.NewRedisQueue(client)

	srv := NewServer(&ServerOptions{
		Queue: q,
	})

	for _, test := range []struct {
		reqMethod string
		reqURL    string
		reqBody   string
		respCode  int
		respBody  string
	}{
		{
			// bad route
			reqMethod: "GET",
			reqURL:    "http://example.com/xxx",
			respCode:  404,
			respBody:  "404 page not found\n",
		},
		{
			// bad method
			reqMethod: "PUT",
			reqURL:    "http://example.com/jobs",
			respCode:  405,
			respBody:  "Method Not Allowed\n",
		},
		{
			// bad method
			reqMethod: "PUT",
			reqURL:    "http://example.com/metrics",
			respCode:  405,
			respBody:  "Method Not Allowed\n",
		},
		{
			// missing required query parameter
			reqMethod: "GET",
			reqURL:    "http://example.com/jobs",
			respCode:  400,
			respBody:  "{\"error\":\"Query argument namespace is required, but not found\"}\n",
		},
		{
			// missing required query parameter
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs",
			respCode:  400,
			respBody:  "{\"error\":\"Query argument namespace is required, but not found\"}\n",
		},
		{
			// bad body
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			respCode:  400,
			respBody:  "{\"error\":\"can't decode JSON body: EOF\"}\n",
		},
		{
			// missing required body fields
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody:   "{}",
			respCode:  400,
			respBody:  "{\"error\":\"namespace and queue_id are required\"}\n",
		},
		{
			// missing required query parameter
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics",
			respCode:  400,
			respBody:  "{\"error\":\"Query argument namespace is required, but not found\"}\n",
		},
		{
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1&job_id=xxx",
			respCode:  200,
			respBody:  "{\"job\":{\"created_at\":\"0001-01-01T00:00:00Z\",\"enqueued_at\":\"0001-01-01T00:00:00Z\",\"id\":\"xxx\",\"last_error\":\"\",\"payload\":null,\"retries\":0,\"updated_at\":\"0001-01-01T00:00:00Z\"},\"namespace\":\"{ns1}\",\"queue_id\":\"q1\"}\n",
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&job_id=xxx",
			respCode:  200,
			respBody:  "{\"job\":{\"created_at\":\"0001-01-01T00:00:00Z\",\"enqueued_at\":\"0001-01-01T00:00:00Z\",\"id\":\"xxx\",\"last_error\":\"\",\"payload\":null,\"retries\":0,\"updated_at\":\"0001-01-01T00:00:00Z\"},\"namespace\":\"{ns1}\",\"status\":\"completed\"}\n",
		},
		{
			// bad duration
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody: `{
				"delay": 1
			}`,
			respCode: 400,
			respBody: "{\"error\":\"can't decode JSON body: invalid duration: 1\"}\n",
		},
		{
			// bad payload
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody:   `{`,
			respCode:  400,
			respBody:  "{\"error\":\"can't decode JSON body: unexpected EOF\"}\n",
		},
		{
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody: `{
				"namespace": "{ns1}",
				"queue_id": "q1",
				"payload": "InBheWxvYWQxIg==",
				"delay": "10s"
			}`,
			respCode: 200,
			respBody: "{\"job\":{\"created_at\":\"[TZ0-9:.+-]+\",\"enqueued_at\":\"[TZ0-9:.+-]+\",\"id\":\"[a-z0-9-]{36}\",\"last_error\":\"\",\"payload\":\"InBheWxvYWQxIg==\",\"retries\":0,\"updated_at\":\"[TZ0-9:.+-]+\"},\"namespace\":\"{ns1}\",\"queue_id\":\"q1\"}",
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  200,
			respBody:  "{\"latency\":0,\"namespace\":\"{ns1}\",\"queue_id\":\"q1\",\"ready_total\":0,\"scheduled_total\":1,\"total\":1}\n",
		},
		{
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody: `{
				"namespace": "{ns1}",
				"queue_id": "q1",
				"payload": "InBheWxvYWQxIg=="
			}`,
			respCode: 200,
			respBody: "{\"job\":{\"created_at\":\"[TZ0-9:.+-]+\",\"enqueued_at\":\"[TZ0-9:.+-]+\",\"id\":\"[a-z0-9-]{36}\",\"last_error\":\"\",\"payload\":\"InBheWxvYWQxIg==\",\"retries\":0,\"updated_at\":\"[TZ0-9:.+-]+\"},\"namespace\":\"{ns1}\",\"queue_id\":\"q1\"}",
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  200,
			respBody:  "{\"latency\":[0-9]+,\"namespace\":\"{ns1}\",\"queue_id\":\"q1\",\"ready_total\":1,\"scheduled_total\":1,\"total\":2}\n",
		},
		{
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody: `{
				"namespace": "{ns1}",
				"queue_id": "q1",
				"id": "id1",
				"payload": "InBheWxvYWQxIg=="
			}`,
			respCode: 200,
			respBody: "{\"job\":{\"created_at\":\"[TZ0-9:.+-]+\",\"enqueued_at\":\"[TZ0-9:.+-]+\",\"id\":\"id1\",\"last_error\":\"\",\"payload\":\"InBheWxvYWQxIg==\",\"retries\":0,\"updated_at\":\"[TZ0-9:.+-]+\"},\"namespace\":\"{ns1}\",\"queue_id\":\"q1\"}",
		},
		{
			// same job id
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody: `{
				"namespace": "{ns1}",
				"queue_id": "q1",
				"id": "id1",
				"payload": "InBheWxvYWQyIg=="
			}`,
			respCode: 200,
			respBody: "{\"job\":{\"created_at\":\"[TZ0-9:.+-]+\",\"enqueued_at\":\"[TZ0-9:.+-]+\",\"id\":\"id1\",\"last_error\":\"\",\"payload\":\"InBheWxvYWQxIg==\",\"retries\":0,\"updated_at\":\"[TZ0-9:.+-]+\"},\"namespace\":\"{ns1}\",\"queue_id\":\"q1\"}",
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  200,
			respBody:  "{\"latency\":[0-9]+,\"namespace\":\"{ns1}\",\"queue_id\":\"q1\",\"ready_total\":2,\"scheduled_total\":1,\"total\":3}\n",
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&job_id=id1",
			respCode:  200,
			respBody:  "{\"job\":{\"created_at\":\"[TZ0-9:.+-]+\",\"enqueued_at\":\"[TZ0-9:.+-]+\",\"id\":\"id1\",\"last_error\":\"\",\"payload\":\"InBheWxvYWQxIg==\",\"retries\":0,\"updated_at\":\"[TZ0-9:.+-]+\"},\"namespace\":\"{ns1}\",\"status\":\"ready\"}",
		},
		{
			// missing required query parameter
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&job_id=id1",
			respCode:  400,
			respBody:  "{\"error\":\"Query argument queue_id is required, but not found\"}\n",
		},
		{
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1&job_id=id1",
			respCode:  200,
			respBody:  "{\"job\":{\"created_at\":\"[TZ0-9:.+-]+\",\"enqueued_at\":\"[TZ0-9:.+-]+\",\"id\":\"id1\",\"last_error\":\"\",\"payload\":\"InBheWxvYWQxIg==\",\"retries\":0,\"updated_at\":\"[TZ0-9:.+-]+\"},\"namespace\":\"{ns1}\",\"queue_id\":\"q1\"}",
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  200,
			respBody:  "{\"latency\":[0-9]+,\"namespace\":\"{ns1}\",\"queue_id\":\"q1\",\"ready_total\":1,\"scheduled_total\":1,\"total\":2}\n",
		},
		{
			// payload is symmetric base64: the exact bytes sent come back
			// unchanged. eyJhIjoxfQ== is base64 for {"a":1}.
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody: `{
				"namespace": "{ns1}",
				"queue_id": "q1",
				"payload": "eyJhIjoxfQ=="
			}`,
			respCode: 200,
			respBody: "{\"job\":{\"created_at\":\"[TZ0-9:.+-]+\",\"enqueued_at\":\"[TZ0-9:.+-]+\",\"id\":\"[a-z0-9-]{36}\",\"last_error\":\"\",\"payload\":\"eyJhIjoxfQ==\",\"retries\":0,\"updated_at\":\"[TZ0-9:.+-]+\"},\"namespace\":\"{ns1}\",\"queue_id\":\"q1\"}",
		},
	} {
		var reqBody io.Reader
		if test.reqBody != "" {
			reqBody = strings.NewReader(test.reqBody)
		}
		req := httptest.NewRequest(test.reqMethod, test.reqURL, reqBody)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		require.Regexp(t, fmt.Sprintf("^%s\n?$", test.respBody), w.Body.String())
		require.Equal(t, test.respCode, w.Code)
	}
}

// bareQueue implements work.Queue but neither BulkJobFinder nor
// MetricsExporter, so capability-gated operations must report 404 while enqueue
// still works.
type bareQueue struct{}

func (bareQueue) Enqueue(*work.Job, *work.EnqueueOptions) error   { return nil }
func (bareQueue) Dequeue(*work.DequeueOptions) (*work.Job, error) { return nil, work.ErrEmptyQueue }
func (bareQueue) Ack(*work.Job, *work.AckOptions) error           { return nil }

func TestServerNotSupported(t *testing.T) {
	srv := NewServer(&ServerOptions{Queue: bareQueue{}})

	for _, test := range []struct {
		reqMethod string
		reqURL    string
		reqBody   string
		respCode  int
		respBody  string
	}{
		{
			// getJob needs BulkJobFinder
			reqMethod: "GET",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&job_id=x",
			respCode:  404,
			respBody:  "",
		},
		{
			// deleteJob needs BulkJobFinder
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1&job_id=x",
			respCode:  404,
			respBody:  "",
		},
		{
			// getMetrics needs MetricsExporter
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  404,
			respBody:  "",
		},
		{
			// enqueue needs no extra capability; the dedup check is skipped when
			// the queue is not a BulkJobFinder, so create still succeeds
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody: `{
				"namespace": "{ns1}",
				"queue_id": "q1",
				"id": "x"
			}`,
			respCode: 200,
			respBody: "{\"job\":{\"created_at\":\"[TZ0-9:.+-]+\",\"enqueued_at\":\"[TZ0-9:.+-]+\",\"id\":\"x\",\"last_error\":\"\",\"payload\":null,\"retries\":0,\"updated_at\":\"[TZ0-9:.+-]+\"},\"namespace\":\"{ns1}\",\"queue_id\":\"q1\"}",
		},
	} {
		var reqBody io.Reader
		if test.reqBody != "" {
			reqBody = strings.NewReader(test.reqBody)
		}
		req := httptest.NewRequest(test.reqMethod, test.reqURL, reqBody)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		require.Regexp(t, fmt.Sprintf("^%s\n?$", test.respBody), w.Body.String())
		require.Equal(t, test.respCode, w.Code)
	}
}

func TestJobStatus(t *testing.T) {
	require.Equal(t, Completed, jobStatus(&work.Job{}))
	require.Equal(t, Ready, jobStatus(work.NewJob()))
	require.Equal(t, Scheduled, jobStatus(work.NewJob().Delay(10*time.Second)))
}
