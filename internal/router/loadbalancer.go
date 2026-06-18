package router

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go-cache-proxy/internal/storage"
)

type LoadBalancerStrategy string

const (
	StrategyRoundRobin LoadBalancerStrategy = "round-robin"
	StrategyWeighted   LoadBalancerStrategy = "weighted"
	StrategyRandom     LoadBalancerStrategy = "random"
	StrategyLeastConn  LoadBalancerStrategy = "least-connections"
)

type LoadBalancer struct {
	mu       sync.RWMutex
	strategy LoadBalancerStrategy
	backends []storage.Backend
	counter  uint64
	rng      *rand.Rand
}

func NewLoadBalancer(strategy LoadBalancerStrategy) *LoadBalancer {
	return &LoadBalancer{
		strategy: strategy,
		backends: make([]storage.Backend, 0),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (lb *LoadBalancer) SetBackends(backends []storage.Backend) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.backends = make([]storage.Backend, 0, len(backends))
	for _, b := range backends {
		if b.Healthy() {
			lb.backends = append(lb.backends, b)
		}
	}
}

func (lb *LoadBalancer) AddBackend(backend storage.Backend) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for _, b := range lb.backends {
		if b.ID() == backend.ID() {
			return
		}
	}
	if backend.Healthy() {
		lb.backends = append(lb.backends, backend)
	}
}

func (lb *LoadBalancer) RemoveBackend(id string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for i, b := range lb.backends {
		if b.ID() == id {
			lb.backends = append(lb.backends[:i], lb.backends[i+1:]...)
			return
		}
	}
}

func (lb *LoadBalancer) Next() (storage.Backend, error) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	healthyBackends := lb.getHealthyBackends()
	if len(healthyBackends) == 0 {
		return nil, storage.ErrBackendUnhealthy
	}

	switch lb.strategy {
	case StrategyRoundRobin:
		return lb.roundRobin(healthyBackends)
	case StrategyWeighted:
		return lb.weightedRandom(healthyBackends)
	case StrategyRandom:
		return lb.random(healthyBackends)
	default:
		return lb.weightedRandom(healthyBackends)
	}
}

func (lb *LoadBalancer) getHealthyBackends() []storage.Backend {
	healthy := make([]storage.Backend, 0, len(lb.backends))
	for _, b := range lb.backends {
		if b.Healthy() {
			healthy = append(healthy, b)
		}
	}
	return healthy
}

func (lb *LoadBalancer) roundRobin(backends []storage.Backend) (storage.Backend, error) {
	if len(backends) == 0 {
		return nil, storage.ErrBackendUnhealthy
	}
	n := atomic.AddUint64(&lb.counter, 1)
	idx := int(n % uint64(len(backends)))
	return backends[idx], nil
}

func (lb *LoadBalancer) weightedRandom(backends []storage.Backend) (storage.Backend, error) {
	if len(backends) == 0 {
		return nil, storage.ErrBackendUnhealthy
	}

	totalWeight := 0
	for _, b := range backends {
		weight := b.Weight()
		if weight <= 0 {
			weight = 1
		}
		totalWeight += weight
	}

	if totalWeight <= 0 {
		return lb.random(backends)
	}

	r := lb.rng.Intn(totalWeight)
	for _, b := range backends {
		weight := b.Weight()
		if weight <= 0 {
			weight = 1
		}
		r -= weight
		if r < 0 {
			return b, nil
		}
	}

	return backends[len(backends)-1], nil
}

func (lb *LoadBalancer) random(backends []storage.Backend) (storage.Backend, error) {
	if len(backends) == 0 {
		return nil, storage.ErrBackendUnhealthy
	}
	idx := lb.rng.Intn(len(backends))
	return backends[idx], nil
}

func (lb *LoadBalancer) GetAll() []storage.Backend {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	result := make([]storage.Backend, len(lb.backends))
	copy(result, lb.backends)
	return result
}
