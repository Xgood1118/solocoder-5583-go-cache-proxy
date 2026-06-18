package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/storage"
)

type RedisBackend struct {
	id       string
	name     string
	weight   int
	healthy  bool
	mu       sync.RWMutex
	client   *goredis.Client
	defaultTTL time.Duration
}

func New(id string, name string, weight int, cfg *config.RedisBackendConfig) (*RedisBackend, error) {
	if cfg == nil {
		return nil, fmt.Errorf("redis config is required")
	}

	addr := cfg.Addr
	if addr == "" {
		addr = "localhost:6379"
	}

	dialTimeout := config.ParseDuration(cfg.DialTimeout, 5*time.Second)
	readTimeout := config.ParseDuration(cfg.ReadTimeout, 3*time.Second)
	writeTimeout := config.ParseDuration(cfg.WriteTimeout, 3*time.Second)

	poolSize := cfg.PoolSize
	if poolSize == 0 {
		poolSize = 10
	}
	minIdleConns := cfg.MinIdleConns
	if minIdleConns == 0 {
		minIdleConns = 2
	}

	client := goredis.NewClient(&goredis.Options{
		Addr:         addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
		MinIdleConns: minIdleConns,
	})

	defaultTTL := 5 * time.Minute

	return &RedisBackend{
		id:         id,
		name:       name,
		weight:     weight,
		healthy:    true,
		client:     client,
		defaultTTL: defaultTTL,
	}, nil
}

func (r *RedisBackend) ID() string {
	return r.id
}

func (r *RedisBackend) Type() string {
	return string(config.BackendTypeRedis)
}

func (r *RedisBackend) Name() string {
	return r.name
}

func (r *RedisBackend) Weight() int {
	return r.weight
}

func (r *RedisBackend) Healthy() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.healthy
}

func (r *RedisBackend) SetHealthy(healthy bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.healthy = healthy
}

func (r *RedisBackend) Get(ctx context.Context, key string) (*storage.Entry, error) {
	if !r.Healthy() {
		return nil, storage.ErrBackendUnhealthy
	}

	pipe := r.client.TxPipeline()
	valCmd := pipe.Get(ctx, key)
	verCmd := pipe.Get(ctx, r.versionKey(key))
	_, err := pipe.Exec(ctx)
	if err == goredis.Nil {
		return nil, storage.ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := valCmd.Bytes()
	if err != nil {
		return nil, fmt.Errorf("failed to parse value for key %s: %w", key, err)
	}

	versionStr, err := verCmd.Result()
	if err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("failed to get version for key %s: %w", key, err)
	}

	version := uint64(0)
	if versionStr != "" {
		version, _ = strconv.ParseUint(versionStr, 10, 64)
	}

	return &storage.Entry{
		Key:     key,
		Value:   value,
		Version: version,
	}, nil
}

func (r *RedisBackend) Set(ctx context.Context, entry *storage.Entry) error {
	if !r.Healthy() {
		return storage.ErrBackendUnhealthy
	}

	ttl := entry.TTL
	if ttl <= 0 {
		ttl = r.defaultTTL
	}

	pipe := r.client.TxPipeline()
	pipe.Set(ctx, entry.Key, entry.Value, ttl)
	pipe.Incr(ctx, r.versionKey(entry.Key))
	pipe.Expire(ctx, r.versionKey(entry.Key), ttl)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to set key %s: %w", entry.Key, err)
	}

	return nil
}

func (r *RedisBackend) Delete(ctx context.Context, key string) error {
	if !r.Healthy() {
		return storage.ErrBackendUnhealthy
	}

	pipe := r.client.TxPipeline()
	pipe.Del(ctx, key)
	pipe.Del(ctx, r.versionKey(key))
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}

	return nil
}

func (r *RedisBackend) Exists(ctx context.Context, key string) (bool, error) {
	if !r.Healthy() {
		return false, storage.ErrBackendUnhealthy
	}

	result, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check existence for key %s: %w", key, err)
	}

	return result > 0, nil
}

func (r *RedisBackend) Ping(ctx context.Context) error {
	err := r.client.Ping(ctx).Err()
	if err != nil {
		r.SetHealthy(false)
		return err
	}
	r.SetHealthy(true)
	return nil
}

func (r *RedisBackend) Close() error {
	return r.client.Close()
}

func (r *RedisBackend) Client() *goredis.Client {
	return r.client
}

func (r *RedisBackend) GetAddr() string {
	opts := r.client.Options()
	if opts != nil {
		return opts.Addr
	}
	return ""
}

func (r *RedisBackend) versionKey(key string) string {
	return fmt.Sprintf("%s:version", key)
}
