package tasker

import (
	"context"
	"fmt"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const queueKey = "tasker_queue"

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

//EnqueueTask LPUSH-es JSON payload
func (q *Queue) EnqueueTask(ctx context.Context, name string, args map[string]any) (string, error) {

	id := uuid.NewString()
	payload, _ := json.Marshal(map[string]any{
		"id": id,
		"name": name,
		"args": args,
	})
	if err := q.rdb.LPush(ctx, queueKey, payload).Err(); err!=nil{
		return "",err
	}
	return id,nil
}

//StartWorker spawns N goroutines blocking on BRPOP
func (q *Queue) StartWorker(ctx context.Context, concurrency int) {
	if concurrency < 1 {
		concurrency = 1
	}
	for i := 0; i < concurrency; i++ {
		go q.loop(ctx,i)
	}
}

func (q *Queue) loop(ctx context.Context, id int) {
	for {
		res, err := q.rdb.BRPop(ctx, 0, queueKey).Result()
		if err != nil {
			if ctx.Err() != nil { return } // ctx canceled = shutdown
			fmt.Println("redis error:", err)
			time.Sleep(time.Second)
			continue
		}
		var job struct {
			ID 		string		`json:"id"`
			Name	string		`json:"name"`
			Args  map[string]any	`json:"args"`
		}
		if err := json.Unmarshal([]byte(res[1]), &job); err != nil{
			fmt.Println("bad json:", err)
			continue
		}

		h, ok := registry[job.Name]
		if !ok {
			fmt.Println("no handler for", job.Name)
			continue
		}
		if err := h(ctx, job.Args); err != nil {
			fmt.Printf("[worker %d] job %s error: %v\n", id, job.ID, err)
		}
	}
}
