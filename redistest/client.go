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

// Reset deletes keys belonging to the given namespaces so the caller starts
// from a clean slate. Scoping cleanup to namespaces lets tests in different
// packages run in parallel against the same Redis without one test's reset
// wiping another test's in-progress data (which a FlushAll would do).
//
// Passing no namespaces falls back to FlushAll for legacy callers.
func Reset(client redis.UniversalClient, namespaces ...string) error {
	ctx := context.Background()
	if len(namespaces) == 0 {
		if cc, ok := client.(*redis.ClusterClient); ok {
			return cc.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
				return c.FlushAll(ctx).Err()
			})
		}
		return client.FlushAll(ctx).Err()
	}

	deleteMatching := func(ctx context.Context, c redis.Cmdable, pattern string) error {
		var cursor uint64
		for {
			keys, next, err := c.Scan(ctx, cursor, pattern, 1000).Result()
			if err != nil {
				return err
			}
			if len(keys) > 0 {
				if err := c.Del(ctx, keys...).Err(); err != nil {
					return err
				}
			}
			if next == 0 {
				return nil
			}
			cursor = next
		}
	}

	for _, ns := range namespaces {
		pattern := ns + ":*"
		if cc, ok := client.(*redis.ClusterClient); ok {
			err := cc.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
				return deleteMatching(ctx, c, pattern)
			})
			if err != nil {
				return err
			}
			continue
		}
		if err := deleteMatching(ctx, client, pattern); err != nil {
			return err
		}
	}
	return nil
}
