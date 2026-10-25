package aegis

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// CompressionThreshold is the combined byte threshold above which step payloads
	// and state snapshots are automatically gzip-compressed.
	CompressionThreshold = 4096

	luaReleaseLease = `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
)

// RedisStore implements durable, distributed checkpointing with sub-millisecond reads and distributed fencing.
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisStore creates a RedisStore with connection pooling and explicit network timeouts.
func NewRedisStore(addr string, ttl time.Duration) *RedisStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolSize:     50,
		MinIdleConns: 10,
	})
	return NewRedisStoreWithClient(rdb, ttl)
}

// NewRedisStoreWithClient wraps an existing go-redis client.
func NewRedisStoreWithClient(client *redis.Client, ttl time.Duration) *RedisStore {
	if ttl <= 0 {
		ttl = 48 * time.Hour
	}
	return &RedisStore{
		client: client,
		ttl:    ttl,
	}
}

// AcquireLease prevents split-brain execution when concurrent triggers or webhooks target the same runID.
func (s *RedisStore) AcquireLease(ctx context.Context, runID, workerID string, leaseTTL time.Duration) (bool, error) {
	key := fmt.Sprintf("aegis:lock:%s", runID)
	return s.client.SetNX(ctx, key, workerID, leaseTTL).Result()
}

// ReleaseLease atomically releases the distributed lease using a Lua script to verify ownership.
func (s *RedisStore) ReleaseLease(ctx context.Context, runID, workerID string) error {
	key := fmt.Sprintf("aegis:lock:%s", runID)
	return s.client.Eval(ctx, luaReleaseLease, []string{key}, workerID).Err()
}

// GetStep fetches and deserializes a versioned step record from Redis Hash storage.
func (s *RedisStore) GetStep(ctx context.Context, runID, versionedStepID string) (EventRecord, bool, error) {
	key := fmt.Sprintf("aegis:steps:%s", runID)
	val, err := s.client.HGet(ctx, key, versionedStepID).Bytes()
	if errors.Is(err, redis.Nil) {
		return EventRecord{}, false, nil
	}
	if err != nil {
		return EventRecord{}, false, err
	}

	var rec EventRecord
	if err := json.Unmarshal(val, &rec); err != nil {
		return EventRecord{}, false, err
	}

	// Decompress output payload and state snapshot if compressed before transmission
	if rec.Compressed {
		if len(rec.OutputPayload) > 0 {
			decompressed, err := decompressGzip(rec.OutputPayload)
			if err != nil {
				return EventRecord{}, false, fmt.Errorf("aegis: decompress output: %w", err)
			}
			rec.OutputPayload = decompressed
		}
		if len(rec.StateSnapshot) > 0 {
			decompressedState, err := decompressGzip(rec.StateSnapshot)
			if err != nil {
				return EventRecord{}, false, fmt.Errorf("aegis: decompress state snapshot: %w", err)
			}
			rec.StateSnapshot = decompressedState
		}
		rec.Compressed = false
	}

	return rec, true, nil
}

// SaveStep persists a step record to Redis Hash with automatic payload & state compression and TTL refresh.
func (s *RedisStore) SaveStep(ctx context.Context, runID string, record EventRecord) error {
	key := fmt.Sprintf("aegis:steps:%s", runID)

	// Transparently compress both OutputPayload and StateSnapshot when combined size exceeds threshold
	if len(record.OutputPayload)+len(record.StateSnapshot) > CompressionThreshold {
		outComp, err1 := compressGzip(record.OutputPayload)
		stateComp, err2 := compressGzip(record.StateSnapshot)
		if err1 == nil && err2 == nil {
			record.OutputPayload = outComp
			record.StateSnapshot = stateComp
			record.Compressed = true
		}
	}

	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, key, record.StepID, raw)
	pipe.Expire(ctx, key, s.ttl)
	_, err = pipe.Exec(ctx)
	return err
}

func compressGzip(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(src); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressGzip(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}
