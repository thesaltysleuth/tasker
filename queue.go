package tasker

import (
	"context"
	"fmt"
	"encoding/json"
	"time"
	"errors"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const queueKey = "tasker_queue"

type Queue struct {
	rdb *redis.Client
}

type jobPayload struct {
	ID 				string 					`json:"id"`
	Name 			string 					`json:"name"`
	Args 			map[string]any	`json:"args"`
	Attempts	int 						`json:"attempts"`
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
	payload, _ := json.Marshal(jobPayload{
		ID: id, Name: name, Args: args, Attempts: 3,
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
		var job jobPayload

		if err := json.Unmarshal([]byte(res[1]), &job); err != nil{
			fmt.Println("bad json:", err)
			continue
		}

		h, ok := registry[job.Name]
		if !ok {
			fmt.Println("no handler for", job.Name)
			continue
		}
		err = h(ctx, job.Args) 
		if err != nil {
			if errors.Is(err, ErrRetry) && job.Attempts > 0 {
				job.Attempts--
				raw, _ := json.Marshal(job)
				// Requeue with small delay
				time.Sleep(time.Second)
				_ = q.rdb.LPush(ctx, queueKey, raw).Err()
				fmt.Printf("[worker %d] jobs %s retry (%d left)\n", id, job.ID, job.Attempts)
			} else {
				fmt.Printf("[worker %d] job %s error: %v\n", id, job.ID, err)
			}
		}
	}
}
