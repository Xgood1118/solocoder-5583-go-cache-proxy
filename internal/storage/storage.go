package storage

import (
	"context"
	"errors"
	"time"
)

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrVersionMismatch  = errors.New("version mismatch")
	ErrBackendUnhealthy = errors.New("backend unhealthy")
	ErrCircuitOpen      = errors.New("circuit breaker open")
	ErrValueTooLarge    = errors.New("value too large")
)

type Entry struct {
	Key     string
	Value   []byte
	Version uint64
	TTL     time.Duration
}

type Backend interface {
	ID() string
	Type() string
	Name() string
	Weight() int
	Healthy() bool
	SetHealthy(bool)
	Get(ctx context.Context, key string) (*Entry, error)
	Set(ctx context.Context, entry *Entry) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Ping(ctx context.Context) error
	Close() error
}

type Factory interface {
	Create(id string, config interface{}) (Backend, error)
}

type HealthStatus struct {
	BackendID      string
	Healthy        bool
	LastCheck      time.Time
	SuccessCount   int
	FailureCount   int
	ConsecutiveSuccess int
	ConsecutiveFailure int
}
