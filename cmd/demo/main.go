package main

import (
	"context"
	"fmt"
	"time"


	"github.com/thesaltysleuth/tasker"
)

func main() {
	ctx := context.Background()
	q := tasker.New("localhost:6379", "", 0)

	tasker.RegisterTask("square", func(ctx context.Context, args map[string]any) error {
		n := int(args["n"].(float64)) // JSON numbers decode as float64
		time.Sleep(500 * time.Millisecond)
		fmt.Println("square: ", n*n)
		return nil
	})

	q.StartWorker(ctx, 2)

	for i := 1; i <= 5; i++ {
		_,_ = q.EnqueueTask(ctx, "square", map[string]any{"n":i})
	}

	time.Sleep(5 * time.Second) //let workers finish demo
}
