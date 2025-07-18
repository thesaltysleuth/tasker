package tasker

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)


func TestRetry(t *testing.T) {
	mr, _ := miniredis.Run()
	defer mr.Close()

	q := New(mr.Addr(), "", 0)

	//register handler that fails twice then succeeds
	count := 0
	RegisterTask("flaky",  func(ctx context.Context, args map[string]any) error {
		count ++
		if count < 3 {
			return ErrRetry
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.StartWorker(ctx,1)

	_,_ = q.EnqueueTask(ctx, "flaky", nil)

	//wait up to 2s
	time.Sleep(4 * time.Second)

	if count!=3 {
		t.Fatalf("expected 3 attempts, got %d", count)
	}
}
