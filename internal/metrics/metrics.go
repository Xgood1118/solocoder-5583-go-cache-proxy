package metrics

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	Namespace = "cache_proxy"
)

var (
	instance *Metrics
	once     sync.Once
)

type Metrics struct {
	registry           *prometheus.Registry
	qpsCounter         *prometheus.CounterVec
	latencyHistogram   *prometheus.HistogramVec
	hitCounter         *prometheus.CounterVec
	missCounter        *prometheus.CounterVec
	evictionCounter    *prometheus.CounterVec
	errorCounter       *prometheus.CounterVec
	backendStatusGauge *prometheus.GaugeVec
	circuitBreakerGauge *prometheus.GaugeVec
	evictionGauge      *prometheus.GaugeVec
	valueSizeHistogram *prometheus.HistogramVec
}

func New() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			registry: prometheus.NewRegistry(),
		}
		instance.initMetrics()
	})
	return instance
}

func (m *Metrics) initMetrics() {
	m.qpsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "requests_total",
			Help:      "Total number of requests processed",
		},
		[]string{"backend", "operation", "status"},
	)

	m.latencyHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "request_duration_seconds",
			Help:      "Request latency distributions",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"backend", "operation"},
	)

	m.hitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "cache_hits_total",
			Help:      "Total number of cache hits",
		},
		[]string{"backend"},
	)

	m.missCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "cache_misses_total",
			Help:      "Total number of cache misses",
		},
		[]string{"backend"},
	)

	m.evictionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "backend_evictions_total",
			Help:      "Total number of backend evictions due to health check failures",
		},
		[]string{"backend"},
	)

	m.errorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "errors_total",
			Help:      "Total number of errors by error code",
		},
		[]string{"backend", "operation", "error_code"},
	)

	m.backendStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "backend_status",
			Help:      "Backend instance status (1 = healthy, 0 = unhealthy)",
		},
		[]string{"backend"},
	)

	m.circuitBreakerGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (0 = closed, 1 = open, 2 = half-open)",
		},
		[]string{"backend"},
	)

	m.evictionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "backend_eviction_count",
			Help:      "Current number of evicted backend instances",
		},
		[]string{"backend"},
	)

	m.valueSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "value_size_bytes",
			Help:      "Distribution of value sizes in bytes",
			Buckets:   []float64{1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216},
		},
		[]string{"backend", "operation"},
	)

	m.registry.MustRegister(
		m.qpsCounter,
		m.latencyHistogram,
		m.hitCounter,
		m.missCounter,
		m.evictionCounter,
		m.errorCounter,
		m.backendStatusGauge,
		m.circuitBreakerGauge,
		m.evictionGauge,
		m.valueSizeHistogram,
	)
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *Metrics) RecordRequest(backend, operation, status string) {
	m.qpsCounter.WithLabelValues(backend, operation, status).Inc()
}

func (m *Metrics) RecordLatency(backend, operation string, duration time.Duration) {
	m.latencyHistogram.WithLabelValues(backend, operation).Observe(duration.Seconds())
}

func (m *Metrics) RecordHit(backend string) {
	m.hitCounter.WithLabelValues(backend).Inc()
}

func (m *Metrics) RecordMiss(backend string) {
	m.missCounter.WithLabelValues(backend).Inc()
}

func (m *Metrics) RecordEviction(backend string) {
	m.evictionCounter.WithLabelValues(backend).Inc()
	m.evictionGauge.WithLabelValues(backend).Inc()
}

func (m *Metrics) RecordRecovery(backend string) {
	m.evictionGauge.WithLabelValues(backend).Dec()
}

func (m *Metrics) RecordError(backend, operation string, errorCode int) {
	m.errorCounter.WithLabelValues(backend, operation, strconv.Itoa(errorCode)).Inc()
}

func (m *Metrics) SetBackendStatus(backend string, healthy bool) {
	status := 0.0
	if healthy {
		status = 1.0
	}
	m.backendStatusGauge.WithLabelValues(backend).Set(status)
}

func (m *Metrics) SetCircuitBreakerState(backend string, state int) {
	m.circuitBreakerGauge.WithLabelValues(backend).Set(float64(state))
}

func (m *Metrics) RecordValueSize(backend, operation string, size int64) {
	m.valueSizeHistogram.WithLabelValues(backend, operation).Observe(float64(size))
}

func (m *Metrics) GetHitRate(backend string) float64 {
	hits, _ := m.hitCounter.GetMetricWithLabelValues(backend)
	misses, _ := m.missCounter.GetMetricWithLabelValues(backend)
	var hitCount, missCount float64
	if metric, ok := hits.(prometheus.Metric); ok {
		hitCount = getCounterValue(metric)
	}
	if metric, ok := misses.(prometheus.Metric); ok {
		missCount = getCounterValue(metric)
	}
	total := hitCount + missCount
	if total == 0 {
		return 0
	}
	return hitCount / total
}

func getCounterValue(metric prometheus.Metric) float64 {
	return 0
}

func Get() *Metrics {
	return instance
}
