package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestQueueScore(t *testing.T) {
	// Higher priority => lower score (processed first)
	base := time.Unix(1000, 0)
	s0 := QueueScore(0, base)
	s1 := QueueScore(1, base)
	s2 := QueueScore(2, base)
	if s1 >= s0 || s2 >= s1 {
		t.Errorf("higher priority should have lower score: %f %f %f", s0, s1, s2)
	}
	// Same priority, earlier time => lower score
	earlier := time.Unix(999, 0)
	later := time.Unix(1001, 0)
	se := QueueScore(1, earlier)
	sl := QueueScore(1, later)
	if se >= sl {
		t.Errorf("earlier time should have lower score: %f %f", se, sl)
	}
}

func TestRedisQueue_EnqueueDequeue(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 15})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available, skip integration test")
	}
	defer client.FlushDB(ctx)

	q := NewRedisQueue(client, 5*time.Second)
	taskID := uuid.New()
	if err := q.Enqueue(ctx, taskID, 1, time.Now()); err != nil {
		t.Fatal(err)
	}
	claim, err := q.Dequeue(ctx, "worker-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil {
		t.Fatal("expected claim")
	}
	if claim.TaskID != taskID {
		t.Errorf("expected task %s, got %s", taskID, claim.TaskID)
	}
	if err := q.Complete(ctx, claim.TaskID, claim.WorkerID); err != nil {
		t.Fatal(err)
	}
	claim2, _ := q.Dequeue(ctx, "worker-1", 0)
	if claim2 != nil {
		t.Error("expected nil after complete")
	}
}
