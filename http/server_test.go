package http

import (
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
			respCode:  404,
			respBody:  "",
		},
		{
			// bad method
			reqMethod: "PUT",
			reqURL:    "http://example.com/metrics",
			respCode:  404,
			respBody:  "",
		},
		{
			// bad url query
			reqMethod: "GET",
			reqURL:    "http://example.com/jobs",
			respCode:  500,
			respBody:  "{\"error\":\"work: empty namespace\"}\n",
		},
		{
			// bad url query
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs",
			respCode:  500,
			respBody:  "{\"error\":\"work: empty namespace\"}\n",
		},
		{
			// bad body
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			respCode:  500,
			respBody:  "{\"error\":\"EOF\"}\n",
		},
		{
			// bad url query
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs",
			reqBody:   "{}",
			respCode:  500,
			respBody:  "{\"error\":\"work: empty namespace\"}\n",
		},
		{
			// bad url query
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics",
			respCode:  500,
			respBody:  "{\"error\":\"work: empty namespace\"}\n",
		},
		{
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&job_id=xxx",
			respCode:  200,
			respBody:  "{\"namespace\":\"{ns1}\",\"queue_id\":\"\",\"job\":{\"ID\":\"xxx\",\"CreatedAt\":\"0001-01-01T00:00:00Z\",\"UpdatedAt\":\"0001-01-01T00:00:00Z\",\"EnqueuedAt\":\"0001-01-01T00:00:00Z\",\"Payload\":null,\"Retries\":0,\"LastError\":\"\"}}\n",
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&job_id=xxx",
			respCode:  200,
			respBody:  "{\"namespace\":\"{ns1}\",\"status\":\"completed\",\"job\":{\"ID\":\"xxx\",\"CreatedAt\":\"0001-01-01T00:00:00Z\",\"UpdatedAt\":\"0001-01-01T00:00:00Z\",\"EnqueuedAt\":\"0001-01-01T00:00:00Z\",\"Payload\":null,\"Retries\":0,\"LastError\":\"\"}}\n",
		},
		{
			// bad duration
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1",
			reqBody: `{
				"payload": "payload1",
				"delay": 1
			}`,
			respCode: 500,
			respBody: "{\"error\":\"invalid duration: 1\"}\n",
		},
		{
			// bad payload
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1",
			reqBody:   `{`,
			respCode:  500,
			respBody:  "{\"error\":\"unexpected EOF\"}\n",
		},
		{
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1",
			reqBody: `{
				"payload": "payload1",
				"delay": "10s"
			}`,
			respCode: 200,
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  200,
			respBody:  "{\"namespace\":\"{ns1}\",\"queue_id\":\"q1\",\"ready_total\":0,\"scheduled_total\":1,\"total\":1}\n",
		},
		{
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1",
			reqBody: `{
				"payload": "payload1"
			}`,
			respCode: 200,
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  200,
			respBody:  "{\"namespace\":\"{ns1}\",\"queue_id\":\"q1\",\"ready_total\":1,\"scheduled_total\":1,\"total\":2}\n",
		},
		{
			reqMethod: "POST",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1",
			reqBody: `{
				"id": "id1",
				"payload": "payload1"
			}`,
			respCode: 200,
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  200,
			respBody:  "{\"namespace\":\"{ns1}\",\"queue_id\":\"q1\",\"ready_total\":2,\"scheduled_total\":1,\"total\":3}\n",
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&job_id=id1",
			respCode:  200,
		},
		{
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&job_id=id1",
			respCode:  500,
			respBody:  "{\"error\":\"work: empty queue id\"}\n",
		},
		{
			reqMethod: "DELETE",
			reqURL:    "http://example.com/jobs?namespace=%7Bns1%7D&queue_id=q1&job_id=id1",
			respCode:  200,
		},
		{
			reqMethod: "GET",
			reqURL:    "http://example.com/metrics?namespace=%7Bns1%7D&queue_id=q1",
			respCode:  200,
			respBody:  "{\"namespace\":\"{ns1}\",\"queue_id\":\"q1\",\"ready_total\":1,\"scheduled_total\":1,\"total\":2}\n",
		},
	} {
		var reqBody io.Reader
		if test.reqBody != "" {
			reqBody = strings.NewReader(test.reqBody)
		}
		req := httptest.NewRequest(test.reqMethod, test.reqURL, reqBody)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		if !(test.respBody == "" && test.respCode == 200) {
			require.Equal(t, test.respBody, w.Body.String())
		} else {
			t.Logf("response: %q", w.Body.String())
		}
		require.Equal(t, test.respCode, w.Code)
	}
}

func TestJobStatus(t *testing.T) {
	require.Equal(t, "completed", jobStatus(&work.Job{}))
	require.Equal(t, "ready", jobStatus(work.NewJob()))
	require.Equal(t, "scheduled", jobStatus(work.NewJob().Delay(10*time.Second)))
}
