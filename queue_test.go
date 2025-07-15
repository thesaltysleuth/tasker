package tasker

import (
	"context"
	"testing"
)


func TestPing(t *testing.T) {
	q := New("localhost:6379","",0)
	if err := q.Ping(context.Background()); err!=nil{
		t.Fatalf("redis ping failed: %v",err)
	}
}


