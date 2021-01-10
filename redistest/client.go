package redistest

import (
	"os"
	"strings"

	"github.com/go-redis/redis/v7"
)

// NewClient creates a redis client for testing.
// REDIS_ADDR contains a list of comma-separated redis addresses.
// If there are more than 1 address, cluster is enabled.
func NewClient() redis.UniversalClient {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	return redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        strings.Split(redisAddr, ","),
		PoolSize:     10,
		MinIdleConns: 10,
	})
}

// Reset is used to clear redis for next test.
func Reset(client redis.UniversalClient) error {
	if cc, ok := client.(*redis.ClusterClient); ok {
		return cc.ForEachMaster(func(c *redis.Client) error {
			return c.FlushAll().Err()
		})
	}
	return client.FlushAll().Err()
}
