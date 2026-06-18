package cache

import (
	"context"
	"sync"
	"time"

	"go-cache-proxy/internal/circuit"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/logger"
	"go-cache-proxy/internal/metrics"
	"go-cache-proxy/internal/router"
	"go-cache-proxy/internal/storage"
)

type CacheService struct {
	mu               sync.RWMutex
	config           *config.CacheConfig
	router           *router.Router
	loadBalancer     *router.LoadBalancer
	circuitManager   *circuit.CircuitBreakerManager
	metrics          *metrics.Metrics
	localCache       storage.Backend
	defaultTTL       time.Duration
	maxValueSize     int64
	enableVersioning bool
	mode             config.ReadWriteMode
	writeBehindQueue chan *writeBehindItem
	wg               sync.WaitGroup
	stopChan         chan struct{}
}

type writeBehindItem struct {
	entry  *storage.Entry
	retry  int
}

type GetOptions struct {
	Version      uint64
	RoutingHint  string
	SkipCache    bool
}

type SetOptions struct {
	Version      uint64
	RoutingHint  string
	TTL          time.Duration
	Mode         config.ReadWriteMode
	CheckVersion bool
}

type DeleteOptions struct {
	RoutingHint string
}

func New(cfg *config.CacheConfig, r *router.Router, lb *router.LoadBalancer, cm *circuit.CircuitBreakerManager, m *metrics.Metrics, localCache storage.Backend) *CacheService {
	defaultTTL := config.ParseDuration(cfg.DefaultTTL, 5*time.Minute)
	maxValueSize := cfg.MaxValueSize
	if maxValueSize == 0 {
		maxValueSize = 1 * 1024 * 1024
	}

	cs := &CacheService{
		config:           cfg,
		router:           r,
		loadBalancer:     lb,
		circuitManager:   cm,
		metrics:          m,
		localCache:       localCache,
		defaultTTL:       defaultTTL,
		maxValueSize:     maxValueSize,
		enableVersioning: cfg.EnableVersioning,
		mode:             cfg.DefaultMode,
		writeBehindQueue: make(chan *writeBehindItem, 10000),
		stopChan:         make(chan struct{}),
	}

	cs.startWriteBehindWorker()

	return cs
}

func (c *CacheService) Get(ctx context.Context, key string, opts *GetOptions) (*storage.Entry, error) {
	start := time.Now()
	defer func() {
		if c.metrics != nil {
			c.metrics.RecordLatency("cache", "get", time.Since(start))
		}
	}()

	if opts == nil {
		opts = &GetOptions{}
	}

	valueSize := int64(0)
	defer func() {
		if c.metrics != nil {
			c.metrics.RecordValueSize("cache", "get", valueSize)
		}
	}()

	if !opts.SkipCache && c.localCache != nil {
		entry, err := c.localCache.Get(ctx, key)
		if err == nil && entry != nil {
			if c.metrics != nil {
				c.metrics.RecordHit("local")
				c.metrics.RecordRequest("local", "get", "hit")
			}
			valueSize = int64(len(entry.Value))

			if c.enableVersioning && opts.Version > 0 && entry.Version != opts.Version {
				return nil, storage.ErrVersionMismatch
			}

			return entry, nil
		}
	}

	if c.metrics != nil {
		c.metrics.RecordMiss("local")
	}

	backends := c.router.GetBackendsForRead(key, opts.RoutingHint)
	if len(backends) == 0 {
		if c.metrics != nil {
			c.metrics.RecordError("cache", "get", 503)
			c.metrics.RecordRequest("cache", "get", "error")
		}
		return nil, storage.ErrBackendUnhealthy
	}

	var lastErr error
	for _, backend := range backends {
		if !c.circuitManager.Execute(backend.ID(), func() error {
			return nil
		}) {
			lastErr = circuit.ErrCircuitOpen
			continue
		}

		entry, err := backend.Get(ctx, key)
		if err == nil && entry != nil {
			if c.metrics != nil {
				c.metrics.RecordHit(backend.ID())
				c.metrics.RecordRequest(backend.ID(), "get", "success")
			}
			valueSize = int64(len(entry.Value))

			if c.enableVersioning && opts.Version > 0 && entry.Version != opts.Version {
				return nil, storage.ErrVersionMismatch
			}

			if c.localCache != nil && !opts.SkipCache && valueSize <= c.maxValueSize {
				go func() {
					_ = c.localCache.Set(ctx, entry)
				}()
			}

			return entry, nil
		}

		if err != storage.ErrKeyNotFound {
			lastErr = err
			if c.metrics != nil {
				c.metrics.RecordError(backend.ID(), "get", 500)
			}
		} else {
			if c.metrics != nil {
				c.metrics.RecordMiss(backend.ID())
			}
		}
	}

	if lastErr != nil {
		if c.metrics != nil {
			c.metrics.RecordRequest("cache", "get", "error")
		}
		return nil, lastErr
	}

	if c.metrics != nil {
		c.metrics.RecordRequest("cache", "get", "miss")
	}
	return nil, storage.ErrKeyNotFound
}

func (c *CacheService) Set(ctx context.Context, entry *storage.Entry, opts *SetOptions) error {
	start := time.Now()
	defer func() {
		if c.metrics != nil {
			c.metrics.RecordLatency("cache", "set", time.Since(start))
		}
	}()

	if opts == nil {
		opts = &SetOptions{}
	}

	valueSize := int64(len(entry.Value))
	if c.metrics != nil {
		c.metrics.RecordValueSize("cache", "set", valueSize)
	}

	mode := opts.Mode
	if mode == "" {
		mode = c.mode
	}

	if valueSize > c.maxValueSize {
		logger.Warn().
			Str("key", entry.Key).
			Int64("size", valueSize).
			Int64("max_size", c.maxValueSize).
			Msg("Value too large, passthrough without caching")
		return c.setPassthrough(ctx, entry, opts)
	}

	if opts.CheckVersion && opts.Version > 0 {
		if c.localCache != nil {
			existing, err := c.localCache.Get(ctx, entry.Key)
			if err == nil && existing.Version != opts.Version {
				if c.metrics != nil {
					c.metrics.RecordError("cache", "set", 409)
					c.metrics.RecordRequest("cache", "set", "version_mismatch")
				}
				return storage.ErrVersionMismatch
			}
		}
	}

	entry.TTL = c.getTTL(entry.TTL, opts.TTL)

	switch mode {
	case config.ReadWriteModeWriteThrough:
		return c.setWriteThrough(ctx, entry, opts)
	case config.ReadWriteModeWriteBehind:
		return c.setWriteBehind(ctx, entry, opts)
	default:
		return c.setCacheAside(ctx, entry, opts)
	}
}

func (c *CacheService) setCacheAside(ctx context.Context, entry *storage.Entry, opts *SetOptions) error {
	if c.localCache != nil {
		if err := c.localCache.Set(ctx, entry); err != nil {
			logger.Warn().Str("key", entry.Key).Err(err).Msg("Failed to write to local cache")
		}
	}

	backend, err := c.router.GetBackend(entry.Key, opts.RoutingHint)
	if err != nil {
		if c.metrics != nil {
			c.metrics.RecordError("cache", "set", 503)
			c.metrics.RecordRequest("cache", "set", "error")
		}
		return err
	}

	if !c.circuitManager.Execute(backend.ID(), func() error {
		return nil
	}) {
		if c.metrics != nil {
			c.metrics.RecordError(backend.ID(), "set", 503)
			c.metrics.RecordRequest(backend.ID(), "set", "circuit_open")
		}
		return circuit.ErrCircuitOpen
	}

	if err := backend.Set(ctx, entry); err != nil {
		if c.metrics != nil {
			c.metrics.RecordError(backend.ID(), "set", 500)
			c.metrics.RecordRequest(backend.ID(), "set", "error")
		}
		return err
	}

	if c.metrics != nil {
		c.metrics.RecordRequest(backend.ID(), "set", "success")
	}
	return nil
}

func (c *CacheService) setWriteThrough(ctx context.Context, entry *storage.Entry, opts *SetOptions) error {
	backend, err := c.router.GetBackend(entry.Key, opts.RoutingHint)
	if err != nil {
		if c.metrics != nil {
			c.metrics.RecordError("cache", "set", 503)
			c.metrics.RecordRequest("cache", "set", "error")
		}
		return err
	}

	if !c.circuitManager.Execute(backend.ID(), func() error {
		return nil
	}) {
		if c.metrics != nil {
			c.metrics.RecordError(backend.ID(), "set", 503)
			c.metrics.RecordRequest(backend.ID(), "set", "circuit_open")
		}
		return circuit.ErrCircuitOpen
	}

	if err := backend.Set(ctx, entry); err != nil {
		if c.metrics != nil {
			c.metrics.RecordError(backend.ID(), "set", 500)
			c.metrics.RecordRequest(backend.ID(), "set", "error")
		}
		return err
	}

	if c.localCache != nil {
		go func() {
			_ = c.localCache.Set(ctx, entry)
		}()
	}

	if c.metrics != nil {
		c.metrics.RecordRequest(backend.ID(), "set", "success")
	}
	return nil
}

func (c *CacheService) setWriteBehind(ctx context.Context, entry *storage.Entry, opts *SetOptions) error {
	if c.localCache != nil {
		if err := c.localCache.Set(ctx, entry); err != nil {
			logger.Warn().Str("key", entry.Key).Err(err).Msg("Failed to write to local cache")
		}
	}

	select {
	case c.writeBehindQueue <- &writeBehindItem{entry: entry, retry: 0}:
		if c.metrics != nil {
			c.metrics.RecordRequest("cache", "set", "queued")
		}
		return nil
	default:
		logger.Warn().Str("key", entry.Key).Msg("Write-behind queue full, falling back to write-through")
		return c.setWriteThrough(ctx, entry, opts)
	}
}

func (c *CacheService) setPassthrough(ctx context.Context, entry *storage.Entry, opts *SetOptions) error {
	backend, err := c.router.GetBackend(entry.Key, opts.RoutingHint)
	if err != nil {
		if c.metrics != nil {
			c.metrics.RecordError("cache", "set", 503)
		}
		return err
	}

	if !c.circuitManager.Execute(backend.ID(), func() error {
		return nil
	}) {
		return circuit.ErrCircuitOpen
	}

	err = backend.Set(ctx, entry)
	if err != nil && c.metrics != nil {
		c.metrics.RecordError(backend.ID(), "set", 500)
	}
	return err
}

func (c *CacheService) Delete(ctx context.Context, key string, opts *DeleteOptions) error {
	start := time.Now()
	defer func() {
		if c.metrics != nil {
			c.metrics.RecordLatency("cache", "delete", time.Since(start))
		}
	}()

	if opts == nil {
		opts = &DeleteOptions{}
	}

	if c.localCache != nil {
		_ = c.localCache.Delete(ctx, key)
	}

	backend, err := c.router.GetBackend(key, opts.RoutingHint)
	if err != nil {
		if c.metrics != nil {
			c.metrics.RecordError("cache", "delete", 503)
		}
		return err
	}

	if !c.circuitManager.Execute(backend.ID(), func() error {
		return nil
	}) {
		return circuit.ErrCircuitOpen
	}

	err = backend.Delete(ctx, key)
	if err != nil && c.metrics != nil {
		c.metrics.RecordError(backend.ID(), "delete", 500)
	}
	return err
}

func (c *CacheService) startWriteBehindWorker() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		batch := make([]*writeBehindItem, 0, 100)

		for {
			select {
			case <-c.stopChan:
				for len(c.writeBehindQueue) > 0 {
					item := <-c.writeBehindQueue
					batch = append(batch, item)
				}
				if len(batch) > 0 {
					c.processWriteBehindBatch(batch)
				}
				return
			case item := <-c.writeBehindQueue:
				batch = append(batch, item)
				if len(batch) >= 100 {
					c.processWriteBehindBatch(batch)
					batch = batch[:0]
				}
			case <-ticker.C:
				if len(batch) > 0 {
					c.processWriteBehindBatch(batch)
					batch = batch[:0]
				}
			}
		}
	}()
}

func (c *CacheService) processWriteBehindBatch(batch []*writeBehindItem) {
	for _, item := range batch {
		c.processWriteBehindItem(item)
	}
}

func (c *CacheService) processWriteBehindItem(item *writeBehindItem) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	backend, err := c.router.GetBackend(item.entry.Key, "")
	if err != nil {
		if item.retry < 3 {
			item.retry++
			go func() {
				time.Sleep(time.Duration(item.retry) * time.Second)
				c.writeBehindQueue <- item
			}()
		} else {
			logger.Error().
				Str("key", item.entry.Key).
				Err(err).
				Msg("Write-behind failed after retries")
		}
		return
	}

	err = backend.Set(ctx, item.entry)
	if err != nil {
		if item.retry < 3 {
			item.retry++
			go func() {
				time.Sleep(time.Duration(item.retry) * time.Second)
				c.writeBehindQueue <- item
			}()
		} else {
			logger.Error().
				Str("key", item.entry.Key).
				Str("backend", backend.ID()).
				Err(err).
				Msg("Write-behind failed after retries")
		}
	}
}

func (c *CacheService) getTTL(ttl ...time.Duration) time.Duration {
	for _, t := range ttl {
		if t > 0 {
			return t
		}
	}
	return c.defaultTTL
}

func (c *CacheService) InvalidateLocal(key string) {
	if c.localCache != nil {
		ctx := context.Background()
		_ = c.localCache.Delete(ctx, key)
		logger.Debug().Str("key", key).Msg("Local cache invalidated")
	}
}

func (c *CacheService) Close() {
	close(c.stopChan)
	c.wg.Wait()
}
