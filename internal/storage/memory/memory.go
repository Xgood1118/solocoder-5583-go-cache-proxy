package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/storage"
)

type MemoryBackend struct {
	id       string
	name     string
	weight   int
	healthy  bool
	mu       sync.RWMutex
	cache    *ristretto.Cache
	versions map[string]uint64
	defaultTTL time.Duration
	versionMu sync.RWMutex
}

func New(id string, name string, weight int, cfg *config.MemoryBackendConfig) (*MemoryBackend, error) {
	if cfg == nil {
		cfg = &config.MemoryBackendConfig{
			MaxCost:     1 << 30,
			NumCounters: 1e7,
			BufferItems: 64,
			DefaultTTL:  "5m",
		}
	}

	maxCost := cfg.MaxCost
	if maxCost == 0 {
		maxCost = 1 << 30
	}
	numCounters := cfg.NumCounters
	if numCounters == 0 {
		numCounters = 1e7
	}
	bufferItems := cfg.BufferItems
	if bufferItems == 0 {
		bufferItems = 64
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: numCounters,
		MaxCost:     maxCost,
		BufferItems: bufferItems,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ristretto cache: %w", err)
	}

	defaultTTL := config.ParseDuration(cfg.DefaultTTL, 5*time.Minute)

	return &MemoryBackend{
		id:         id,
		name:       name,
		weight:     weight,
		healthy:    true,
		cache:      cache,
		versions:   make(map[string]uint64),
		defaultTTL: defaultTTL,
	}, nil
}

func (m *MemoryBackend) ID() string {
	return m.id
}

func (m *MemoryBackend) Type() string {
	return string(config.BackendTypeMemory)
}

func (m *MemoryBackend) Name() string {
	return m.name
}

func (m *MemoryBackend) Weight() int {
	return m.weight
}

func (m *MemoryBackend) Healthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthy
}

func (m *MemoryBackend) SetHealthy(healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthy = healthy
}

func (m *MemoryBackend) Get(ctx context.Context, key string) (*storage.Entry, error) {
	if !m.Healthy() {
		return nil, storage.ErrBackendUnhealthy
	}

	val, found := m.cache.Get(key)
	if !found {
		return nil, storage.ErrKeyNotFound
	}

	value, ok := val.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid value type for key %s", key)
	}

	version := m.getVersion(key)

	return &storage.Entry{
		Key:     key,
		Value:   value,
		Version: version,
	}, nil
}

func (m *MemoryBackend) Set(ctx context.Context, entry *storage.Entry) error {
	if !m.Healthy() {
		return storage.ErrBackendUnhealthy
	}

	ttl := entry.TTL
	if ttl <= 0 {
		ttl = m.defaultTTL
	}

	m.incrementVersion(entry.Key)

	cost := int64(len(entry.Value))
	ok := m.cache.SetWithTTL(entry.Key, entry.Value, cost, ttl)
	if !ok {
		return fmt.Errorf("failed to set key %s in cache", entry.Key)
	}

	return nil
}

func (m *MemoryBackend) Delete(ctx context.Context, key string) error {
	if !m.Healthy() {
		return storage.ErrBackendUnhealthy
	}

	m.cache.Del(key)
	m.deleteVersion(key)
	return nil
}

func (m *MemoryBackend) Exists(ctx context.Context, key string) (bool, error) {
	if !m.Healthy() {
		return false, storage.ErrBackendUnhealthy
	}

	_, found := m.cache.Get(key)
	return found, nil
}

func (m *MemoryBackend) Ping(ctx context.Context) error {
	m.SetHealthy(true)
	return nil
}

func (m *MemoryBackend) Close() error {
	m.cache.Close()
	return nil
}

func (m *MemoryBackend) getVersion(key string) uint64 {
	m.versionMu.RLock()
	defer m.versionMu.RUnlock()
	return m.versions[key]
}

func (m *MemoryBackend) incrementVersion(key string) {
	m.versionMu.Lock()
	defer m.versionMu.Unlock()
	m.versions[key]++
}

func (m *MemoryBackend) deleteVersion(key string) {
	m.versionMu.Lock()
	defer m.versionMu.Unlock()
	delete(m.versions, key)
}
