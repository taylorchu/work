package redistest

import (
	"context"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
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
		return cc.ForEachMaster(context.Background(), func(ctx context.Context, c *redis.Client) error {
			return c.FlushAll(ctx).Err()
		})
	}
	return client.FlushAll(context.Background()).Err()
}
