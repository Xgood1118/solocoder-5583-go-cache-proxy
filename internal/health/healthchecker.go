package health

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	gocron "github.com/go-co-op/gocron/v2"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/logger"
	"go-cache-proxy/internal/metrics"
	"go-cache-proxy/internal/storage"
)

type HealthChecker struct {
	mu            sync.RWMutex
	router        interface{}
	metrics       *metrics.Metrics
	scheduler     gocron.Scheduler
	backends      map[string]*backendHealthStatus
	evicted       map[string]bool
}

type backendHealthStatus struct {
	backend             storage.Backend
	config              *config.HealthCheckConfig
	lastCheck           time.Time
	successCount        int
	failureCount        int
	consecutiveSuccess  int
	consecutiveFailure  int
	failureThreshold    int
	successThreshold    int
	interval            time.Duration
	timeout             time.Duration
	checkType           config.HealthCheckType
	jobID               gocron.Job
}

func New(m *metrics.Metrics) (*HealthChecker, error) {
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("failed to create scheduler: %w", err)
	}

	return &HealthChecker{
		metrics:   m,
		scheduler: scheduler,
		backends:  make(map[string]*backendHealthStatus),
		evicted:   make(map[string]bool),
	}, nil
}

func (hc *HealthChecker) RegisterBackend(backend storage.Backend, cfg *config.HealthCheckConfig) error {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if cfg == nil || !cfg.Enabled {
		return nil
	}

	if _, exists := hc.backends[backend.ID()]; exists {
		return nil
	}

	interval := config.ParseDuration(cfg.Interval, 30*time.Second)
	timeout := config.ParseDuration(cfg.Timeout, 5*time.Second)
	failureThreshold := cfg.FailureThreshold
	if failureThreshold == 0 {
		failureThreshold = 3
	}
	successThreshold := cfg.SuccessThreshold
	if successThreshold == 0 {
		successThreshold = 2
	}

	checkType := cfg.Type
	if checkType == "" {
		checkType = config.HealthCheckTypePing
	}

	status := &backendHealthStatus{
		backend:          backend,
		config:           cfg,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		interval:         interval,
		timeout:          timeout,
		checkType:        checkType,
	}

	job, err := hc.scheduler.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(hc.checkBackend, backend.ID()),
		gocron.WithName(fmt.Sprintf("health-check-%s", backend.ID())),
	)
	if err != nil {
		return fmt.Errorf("failed to schedule health check for %s: %w", backend.ID(), err)
	}

	status.jobID = job
	hc.backends[backend.ID()] = status

	logger.Info().
		Str("backend", backend.ID()).
		Str("type", string(checkType)).
		Str("interval", interval.String()).
		Msg("Health check registered")

	return nil
}

func (hc *HealthChecker) UnregisterBackend(id string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if status, exists := hc.backends[id]; exists {
		_ = hc.scheduler.RemoveJob(status.jobID.ID())
		delete(hc.backends, id)
		delete(hc.evicted, id)
		logger.Info().Str("backend", id).Msg("Health check unregistered")
	}
}

func (hc *HealthChecker) checkBackend(id string) {
	hc.mu.RLock()
	status, exists := hc.backends[id]
	hc.mu.RUnlock()

	if !exists {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), status.timeout)
	defer cancel()

	start := time.Now()
	var err error

	switch status.checkType {
	case config.HealthCheckTypeTCP:
		err = hc.checkTCP(status)
	case config.HealthCheckTypePing:
		err = status.backend.Ping(ctx)
	default:
		err = status.backend.Ping(ctx)
	}

	duration := time.Since(start)

	hc.mu.Lock()
	defer hc.mu.Unlock()

	status.lastCheck = time.Now()

	if err == nil {
		status.successCount++
		status.consecutiveSuccess++
		status.consecutiveFailure = 0

		logger.Debug().
			Str("backend", id).
			Str("duration", duration.String()).
			Msg("Health check passed")

		if hc.evicted[id] && status.consecutiveSuccess >= status.successThreshold {
			delete(hc.evicted, id)
			status.backend.SetHealthy(true)
			if hc.metrics != nil {
				hc.metrics.RecordRecovery(id)
				hc.metrics.SetBackendStatus(id, true)
			}
			logger.Warn().
				Str("backend", id).
				Int("consecutive_success", status.consecutiveSuccess).
				Msg("Backend recovered and readmitted")
		} else if !hc.evicted[id] {
			status.backend.SetHealthy(true)
			if hc.metrics != nil {
				hc.metrics.SetBackendStatus(id, true)
			}
		}
	} else {
		status.failureCount++
		status.consecutiveFailure++
		status.consecutiveSuccess = 0

		logger.Warn().
			Str("backend", id).
			Str("duration", duration.String()).
			Err(err).
			Msg("Health check failed")

		if !hc.evicted[id] && status.consecutiveFailure >= status.failureThreshold {
			hc.evicted[id] = true
			status.backend.SetHealthy(false)
			if hc.metrics != nil {
				hc.metrics.RecordEviction(id)
				hc.metrics.SetBackendStatus(id, false)
			}
			logger.Error().
				Str("backend", id).
				Int("consecutive_failure", status.consecutiveFailure).
				Msg("Backend evicted due to health check failures")
		}
	}
}

func (hc *HealthChecker) checkTCP(status *backendHealthStatus) error {
	addr := hc.extractAddr(status.backend)
	if addr == "" {
		return status.backend.Ping(context.Background())
	}

	conn, err := net.DialTimeout("tcp", addr, status.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	return nil
}

func (hc *HealthChecker) extractAddr(backend storage.Backend) string {
	switch backend.Type() {
	case string(config.BackendTypeRedis):
		if b, ok := backend.(interface{ Client() interface{ Options() *struct { Addr string } } }); ok {
			if opts := b.Client().Options(); opts != nil {
				return opts.Addr
			}
		}
	case string(config.BackendTypeHTTP):
		if b, ok := backend.(interface{ GetBaseURL() string }); ok {
			baseURL := b.GetBaseURL()
			if strings.Contains(baseURL, "://") {
				baseURL = strings.Split(baseURL, "://")[1]
			}
			if strings.Contains(baseURL, "/") {
				baseURL = strings.Split(baseURL, "/")[0]
			}
			return baseURL
		}
	case string(config.BackendTypeGRPC):
		if b, ok := backend.(interface{ GetAddr() string }); ok {
			return b.GetAddr()
		}
	}
	return ""
}

func (hc *HealthChecker) Start() {
	hc.scheduler.Start()
	logger.Info().Msg("Health checker started")
}

func (hc *HealthChecker) Stop() {
	_ = hc.scheduler.Shutdown()
	logger.Info().Msg("Health checker stopped")
}

func (hc *HealthChecker) IsEvicted(id string) bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.evicted[id]
}

func (hc *HealthChecker) GetHealthStatus(id string) *storage.HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	status, exists := hc.backends[id]
	if !exists {
		return nil
	}

	return &storage.HealthStatus{
		BackendID:           id,
		Healthy:             status.backend.Healthy(),
		LastCheck:           status.lastCheck,
		SuccessCount:        status.successCount,
		FailureCount:        status.failureCount,
		ConsecutiveSuccess:  status.consecutiveSuccess,
		ConsecutiveFailure:  status.consecutiveFailure,
	}
}

func (hc *HealthChecker) GetAllHealthStatus() map[string]*storage.HealthStatus {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	result := make(map[string]*storage.HealthStatus, len(hc.backends))
	for id, status := range hc.backends {
		result[id] = &storage.HealthStatus{
			BackendID:           id,
			Healthy:             status.backend.Healthy(),
			LastCheck:           status.lastCheck,
			SuccessCount:        status.successCount,
			FailureCount:        status.failureCount,
			ConsecutiveSuccess:  status.consecutiveSuccess,
			ConsecutiveFailure:  status.consecutiveFailure,
		}
	}
	return result
}
