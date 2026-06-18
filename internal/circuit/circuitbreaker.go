package circuit

import (
	"errors"
	"sync"
	"time"

	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/logger"
	"go-cache-proxy/internal/metrics"
)

type State int

const (
	StateClosed    State = 0
	StateOpen      State = 1
	StateHalfOpen  State = 2
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

type CircuitBreaker struct {
	mu                sync.RWMutex
	id                string
	state             State
	config            *config.CircuitBreakerConfig
	failureCount      int
	successCount      int
	halfOpenCalls     int
	lastStateChange   time.Time
	timeout           time.Duration
	metrics           *metrics.Metrics
}

type CircuitBreakerManager struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
	metrics  *metrics.Metrics
}

func NewCircuitBreakerManager(m *metrics.Metrics) *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
		metrics:  m,
	}
}

func (m *CircuitBreakerManager) GetOrCreate(id string, cfg *config.CircuitBreakerConfig) *CircuitBreaker {
	m.mu.RLock()
	cb, exists := m.breakers[id]
	m.mu.RUnlock()

	if exists {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if cb, exists := m.breakers[id]; exists {
		return cb
	}

	if cfg == nil || !cfg.Enabled {
		return nil
	}

	timeout := config.ParseDuration(cfg.Timeout, 30*time.Second)
	cb = &CircuitBreaker{
		id:              id,
		state:           StateClosed,
		config:          cfg,
		timeout:         timeout,
		lastStateChange: time.Now(),
		metrics:         m.metrics,
	}

	m.breakers[id] = cb
	return cb
}

func (cb *CircuitBreaker) Allow() bool {
	if cb == nil {
		return true
	}

	cb.mu.RLock()
	state := cb.state
	cb.mu.RUnlock()

	switch state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.lastStateChange) > cb.timeout {
			cb.mu.Lock()
			if cb.state == StateOpen && time.Since(cb.lastStateChange) > cb.timeout {
				cb.setState(StateHalfOpen)
				cb.halfOpenCalls = 0
			}
			cb.mu.Unlock()
			return cb.state == StateHalfOpen
		}
		return false
	case StateHalfOpen:
		cb.mu.Lock()
		if cb.halfOpenCalls < cb.config.HalfOpenMaxCalls {
			cb.halfOpenCalls++
			cb.mu.Unlock()
			return true
		}
		cb.mu.Unlock()
		return false
	default:
		return true
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.failureCount = 0
	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.config.SuccessThreshold {
			cb.setState(StateClosed)
			cb.failureCount = 0
			cb.successCount = 0
			cb.halfOpenCalls = 0
		}
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.failureCount++
		if cb.failureCount >= cb.config.FailureThreshold {
			cb.setState(StateOpen)
			logger.Warn().
				Str("backend", cb.id).
				Int("failure_count", cb.failureCount).
				Msg("Circuit breaker opened")
		}
	case StateHalfOpen:
		cb.setState(StateOpen)
		cb.failureCount = cb.config.FailureThreshold
		cb.successCount = 0
		cb.halfOpenCalls = 0
		logger.Warn().
			Str("backend", cb.id).
			Msg("Circuit breaker reopened from half-open state")
	}
}

func (cb *CircuitBreaker) State() State {
	if cb == nil {
		return StateClosed
	}
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *CircuitBreaker) setState(state State) {
	oldState := cb.state
	cb.state = state
	cb.lastStateChange = time.Now()

	if oldState != state && cb.metrics != nil {
		cb.metrics.SetCircuitBreakerState(cb.id, int(state))
	}

	logger.Info().
		Str("backend", cb.id).
		Str("old_state", stateToString(oldState)).
		Str("new_state", stateToString(state)).
		Msg("Circuit breaker state changed")
}

func stateToString(state State) string {
	switch state {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

func (m *CircuitBreakerManager) Execute(id string, fn func() error) error {
	cb := m.GetOrCreate(id, nil)
	if !cb.Allow() {
		return ErrCircuitOpen
	}

	err := fn()
	if err != nil {
		cb.RecordFailure()
		return err
	}

	cb.RecordSuccess()
	return nil
}

func (m *CircuitBreakerManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.breakers {
		delete(m.breakers, id)
	}
}
