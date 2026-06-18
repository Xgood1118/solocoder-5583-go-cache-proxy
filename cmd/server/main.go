package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go-cache-proxy/internal/api"
	"go-cache-proxy/internal/cache"
	"go-cache-proxy/internal/circuit"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/gossip"
	"go-cache-proxy/internal/health"
	"go-cache-proxy/internal/logger"
	"go-cache-proxy/internal/metrics"
	"go-cache-proxy/internal/pubsub"
	"go-cache-proxy/internal/router"
	"go-cache-proxy/internal/storage"
	memBackend "go-cache-proxy/internal/storage/memory"
	redisBackend "go-cache-proxy/internal/storage/redis"
	httpBackend "go-cache-proxy/internal/storage/http"
	grpcBackend "go-cache-proxy/internal/storage/grpc"
)

func generateNodeID() string {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return fmt.Sprintf("node-%d", time.Now().Unix())
	}
	return hex.EncodeToString(b)
}

func main() {
	configPath := "configs/config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger.Init(cfg.LogLevel, cfg.LogFormat)
	logger.Info().Msg("Starting cache proxy server...")

	nodeID := generateNodeID()
	logger.Info().Str("node_id", nodeID).Msg("Generated node ID")

	m := metrics.New()

	hc, err := health.New(m)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create health checker")
	}
	defer hc.Stop()

	r := router.New()
	lb := router.NewLoadBalancer(router.StrategyWeighted)
	cm := circuit.NewCircuitBreakerManager(m)

	var localCache storage.Backend
	var backends []storage.Backend

	for _, backendCfg := range cfg.Backends {
		var backend storage.Backend
		var createErr error

		switch backendCfg.Type {
		case config.BackendTypeMemory:
			backend, createErr = memBackend.New(
				backendCfg.ID,
				backendCfg.Name,
				backendCfg.Weight,
				backendCfg.Memory,
			)
			if backendCfg.ID == "local" || len(cfg.Backends) == 1 {
				localCache = backend
			}

		case config.BackendTypeRedis:
			backend, createErr = redisBackend.New(
				backendCfg.ID,
				backendCfg.Name,
				backendCfg.Weight,
				backendCfg.Redis,
			)

		case config.BackendTypeHTTP:
			backend, createErr = httpBackend.New(
				backendCfg.ID,
				backendCfg.Name,
				backendCfg.Weight,
				backendCfg.HTTP,
			)

		case config.BackendTypeGRPC:
			backend, createErr = grpcBackend.New(
				backendCfg.ID,
				backendCfg.Name,
				backendCfg.Weight,
				backendCfg.GRPC,
			)

		default:
			logger.Warn().Str("type", string(backendCfg.Type)).Str("id", backendCfg.ID).Msg("Unknown backend type, skipping")
			continue
		}

		if createErr != nil {
			logger.Error().
				Err(createErr).
				Str("type", string(backendCfg.Type)).
				Str("id", backendCfg.ID).
				Msg("Failed to create backend")
			continue
		}

		backends = append(backends, backend)
		r.RegisterBackend(backend)
		lb.AddBackend(backend)
		cm.GetOrCreate(backend.ID(), backendCfg.CircuitBreaker)

		if backendCfg.HealthCheck != nil && backendCfg.HealthCheck.Enabled {
			if hcErr := hc.RegisterBackend(backend, backendCfg.HealthCheck); hcErr != nil {
				logger.Error().
					Err(hcErr).
					Str("id", backendCfg.ID).
					Msg("Failed to register health check")
			}
		}

		logger.Info().
			Str("type", string(backendCfg.Type)).
			Str("id", backendCfg.ID).
			Str("name", backendCfg.Name).
			Msg("Backend registered")
	}

	if localCache == nil {
		logger.Info().Msg("No local cache backend found, creating default memory cache")
		defaultMemCfg := &config.MemoryBackendConfig{
			MaxCost:     1 << 30,
			NumCounters: 1e7,
			BufferItems: 64,
			DefaultTTL:  "5m",
		}
		localCache, err = memBackend.New("local", "Local Cache", 1, defaultMemCfg)
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to create local cache")
		}
		backends = append(backends, localCache)
		r.RegisterBackend(localCache)
		lb.AddBackend(localCache)
	}

	r.SetRules(cfg.Routing)

	cs := cache.New(&cfg.Cache, r, lb, cm, m, localCache)
	defer cs.Close()

	ps, err := pubsub.NewRedisPubSub(nodeID, &cfg.PubSub)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to initialize Pub/Sub")
	} else if ps != nil {
		defer ps.Close()
		ps.Subscribe(func(key string) {
			cs.InvalidateLocal(key)
		})
		logger.Info().Msg("Pub/Sub invalidation enabled")
	}

	gp, err := gossip.New(nodeID, &cfg.Gossip)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to initialize Gossip protocol")
	} else if gp != nil {
		defer gp.Close()
		gp.Subscribe(func(key string) {
			cs.InvalidateLocal(key)
		})
		logger.Info().Msg("Gossip protocol enabled")
	}

	hc.Start()

	if cfg.LogLevel == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(api.CORSMiddleware())
	engine.Use(api.RequestLogger())
	engine.Use(api.GetRoutingHint())

	handler := api.NewHandler(cs, hc, m, cfg)
	handler.RegisterRoutes(engine)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	readTimeout := config.ParseDuration(cfg.Server.ReadTimeout, 30*time.Second)
	writeTimeout := config.ParseDuration(cfg.Server.WriteTimeout, 30*time.Second)
	idleTimeout := config.ParseDuration(cfg.Server.IdleTimeout, 60*time.Second)

	srv := &http.Server{
		Addr:         addr,
		Handler:      engine,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	go func() {
		logger.Info().Str("addr", addr).Msg("HTTP server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("Failed to start HTTP server")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info().Msg("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error().Err(err).Msg("Server forced to shutdown")
	}

	for _, backend := range backends {
		if err := backend.Close(); err != nil {
			logger.Error().Err(err).Str("id", backend.ID()).Msg("Error closing backend")
		}
	}

	logger.Info().Msg("Server exiting")
}
