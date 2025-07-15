package tasker

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	rdb *redis.Client
}

func New(addr, password string, db int) *Queue {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr, 					// localhost:6379
		Password: password, 	// usually ""
		DB: db,
		PoolSize: 10,
		ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	return &Queue{rdb: rdb}
}

func (q *Queue) Ping(ctx context.Context) error {
	return q.rdb.Ping(ctx).Err()
}




