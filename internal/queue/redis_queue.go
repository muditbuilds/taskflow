package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	maxScore = 1e15
)

type RedisQueue struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisQueue(client *redis.Client, lockTTL time.Duration) *RedisQueue {
	return &RedisQueue{client: client, ttl: lockTTL}
}

// score: higher priority and earlier scheduled_at should be processed first.
// We use sorted set: lower score = higher priority. So score = -priority*1e10 + scheduled_unix.
// That way high priority runs first; within same priority, earlier time first.
func queueScore(priority int, scheduledAt time.Time) float64 {
	return float64(-priority)*1e10 + float64(scheduledAt.Unix())
}

func (q *RedisQueue) Enqueue(ctx context.Context, taskID uuid.UUID, priority int, scheduledAt time.Time) error {
	score := queueScore(priority, scheduledAt)
	return q.client.ZAdd(ctx, QueueName, redis.Z{Score: score, Member: taskID.String()}).Err()
}

// Dequeue implements work-stealing: atomically move one task from queue to processing set with lock.
// We use ZPOPMIN to get lowest score (highest priority + earliest time), then set lock and add to processing.
func (q *RedisQueue) Dequeue(ctx context.Context, workerID string, block time.Duration) (*Claim, error) {
	pipe := q.client.Pipeline()
	// Get one task with lowest score (best priority)
	pipe.ZPopMin(ctx, QueueName, 1)
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	zpop := cmds[0].(*redis.ZSliceCmd)
	results, err := zpop.Result()
	if err != nil || len(results) == 0 {
		return nil, err
	}
	taskIDStr, ok := results[0].Member.(string)
	if !ok {
		return nil, fmt.Errorf("invalid member type")
	}
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		return nil, err
	}
	lockKey := LockPrefix + taskID.String()
	ok, err = q.client.SetNX(ctx, lockKey, workerID, q.ttl).Result()
	if err != nil {
		// Re-add to queue on lock failure (another worker got it or we failed to lock)
		_ = q.client.ZAdd(ctx, QueueName, redis.Z{Score: results[0].Score, Member: taskIDStr}).Err()
		return nil, err
	}
	if !ok {
		_ = q.client.ZAdd(ctx, QueueName, redis.Z{Score: results[0].Score, Member: taskIDStr}).Err()
		return nil, nil
	}
	// Track that this worker is processing this task (for heartbeat / release)
	_ = q.client.HSet(ctx, ProcessingSet, taskIDStr, workerID).Err()
	return &Claim{TaskID: taskID, WorkerID: workerID, LockKey: lockKey}, nil
}

func (q *RedisQueue) Release(ctx context.Context, taskID uuid.UUID, workerID string) error {
	key := LockPrefix + taskID.String()
	val, err := q.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	if val != workerID {
		return nil
	}
	if err := q.client.Del(ctx, key).Err(); err != nil {
		return err
	}
	_ = q.client.HDel(ctx, ProcessingSet, taskID.String()).Err()
	// Re-enqueue: we don't have score here; caller should re-enqueue via scheduler or use a default
	return q.client.ZAdd(ctx, QueueName, redis.Z{Score: 0, Member: taskID.String()}).Err()
}

func (q *RedisQueue) Complete(ctx context.Context, taskID uuid.UUID, workerID string) error {
	key := LockPrefix + taskID.String()
	val, err := q.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	if val != workerID {
		return nil
	}
	if err := q.client.Del(ctx, key).Err(); err != nil {
		return err
	}
	_ = q.client.HDel(ctx, ProcessingSet, taskID.String()).Err()
	return nil
}

func (q *RedisQueue) ExtendLock(ctx context.Context, taskID uuid.UUID, workerID string, ttl time.Duration) error {
	key := LockPrefix + taskID.String()
	val, err := q.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	if val != workerID {
		return nil
	}
	return q.client.Expire(ctx, key, ttl).Err()
}

func (q *RedisQueue) PublishTaskCompleted(ctx context.Context, taskID uuid.UUID) error {
	return q.client.Publish(ctx, ChannelCompleted, taskID.String()).Err()
}

// BlockingDequeue polls until a task is available or context is done.
func (q *RedisQueue) BlockingDequeue(ctx context.Context, workerID string, pollInterval time.Duration) (*Claim, error) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		claim, err := q.Dequeue(ctx, workerID, 0)
		if err != nil {
			return nil, err
		}
		if claim != nil {
			return claim, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			continue
		}
	}
}

// ParseScore is used in tests.
func ParseScore(s float64) (priority int, unix int64) {
	priority = -int(s / 1e10)
	unix = int64(s + float64(priority)*1e10)
	return priority, unix
}

func QueueScore(priority int, scheduledAt time.Time) float64 {
	return queueScore(priority, scheduledAt)
}

// Ensure RedisQueue implements Queue
var _ Queue = (*RedisQueue)(nil)
