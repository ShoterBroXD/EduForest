package store

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	c *redis.Client
}

func NewRedis(ctx context.Context, addr string) (*Redis, error) {
	c := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 2 * time.Second})
	var err error
	for i := 0; i < 10; i++ {
		if err = c.Ping(ctx).Err(); err == nil {
			return &Redis{c: c}, nil
		}
		time.Sleep(time.Second)
	}
	return nil, err
}

func (r *Redis) Ping(ctx context.Context) error { return r.c.Ping(ctx).Err() }
func (r *Redis) Close() error                   { return r.c.Close() }

func (r *Redis) Get(ctx context.Context, key string) ([]byte, bool) {
	b, err := r.c.Get(ctx, key).Bytes()
	return b, err == nil
}

func (r *Redis) Set(ctx context.Context, key string, val []byte, ttl time.Duration) {
	r.c.Set(ctx, key, val, ttl)
}

func (r *Redis) Incr(ctx context.Context, hash, field string) {
	r.c.HIncrBy(ctx, hash, field, 1)
}

func (r *Redis) Counters(ctx context.Context, hash string) map[string]int64 {
	out := map[string]int64{}
	m, err := r.c.HGetAll(ctx, hash).Result()
	if err != nil {
		return out
	}
	for k, v := range m {
		n, _ := strconv.ParseInt(v, 10, 64)
		out[k] = n
	}
	return out
}
