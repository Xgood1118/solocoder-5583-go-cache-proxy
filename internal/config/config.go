package config

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type BackendType string

const (
	BackendTypeMemory BackendType = "memory"
	BackendTypeRedis  BackendType = "redis"
	BackendTypeHTTP   BackendType = "http"
	BackendTypeGRPC   BackendType = "grpc"
)

type RoutingStrategy string

const (
	RoutingStrategyExact    RoutingStrategy = "exact"
	RoutingStrategyWildcard RoutingStrategy = "wildcard"
	RoutingStrategyHash     RoutingStrategy = "hash"
	RoutingStrategyBlacklist RoutingStrategy = "blacklist"
	RoutingStrategyWhitelist RoutingStrategy = "whitelist"
)

type ReadWriteMode string

const (
	ReadWriteModeCacheAside  ReadWriteMode = "cache-aside"
	ReadWriteModeWriteThrough ReadWriteMode = "write-through"
	ReadWriteModeWriteBehind ReadWriteMode = "write-behind"
)

type HealthCheckType string

const (
	HealthCheckTypeTCP  HealthCheckType = "tcp"
	HealthCheckTypePing HealthCheckType = "ping"
)

type PubSubType string

const (
	PubSubTypeRedis PubSubType = "redis"
	PubSubTypeNATS  PubSubType = "nats"
)

type GossipConfig struct {
	Enabled       bool     `mapstructure:"enabled"`
	BindAddr      string   `mapstructure:"bind_addr"`
	BindPort      int      `mapstructure:"bind_port"`
	SeedNodes     []string `mapstructure:"seed_nodes"`
	ProbeInterval string   `mapstructure:"probe_interval"`
	ProbeTimeout  string   `mapstructure:"probe_timeout"`
}

type PubSubConfig struct {
	Enabled  bool       `mapstructure:"enabled"`
	Type     PubSubType `mapstructure:"type"`
	RedisURL string     `mapstructure:"redis_url"`
	NATSURL  string     `mapstructure:"nats_url"`
	Channel  string     `mapstructure:"channel"`
}

type CircuitBreakerConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	FailureThreshold   int    `mapstructure:"failure_threshold"`
	SuccessThreshold   int    `mapstructure:"success_threshold"`
	Timeout            string `mapstructure:"timeout"`
	HalfOpenMaxCalls   int    `mapstructure:"half_open_max_calls"`
}

type HealthCheckConfig struct {
	Enabled             bool              `mapstructure:"enabled"`
	Type                HealthCheckType   `mapstructure:"type"`
	Interval            string            `mapstructure:"interval"`
	Timeout             string            `mapstructure:"timeout"`
	FailureThreshold    int               `mapstructure:"failure_threshold"`
	SuccessThreshold    int               `mapstructure:"success_threshold"`
}

type MemoryBackendConfig struct {
	MaxCost     int64  `mapstructure:"max_cost"`
	NumCounters int64  `mapstructure:"num_counters"`
	BufferItems int64  `mapstructure:"buffer_items"`
	DefaultTTL  string `mapstructure:"default_ttl"`
}

type RedisBackendConfig struct {
	Addr         string `mapstructure:"addr"`
	Password     string `mapstructure:"password"`
	DB           int    `mapstructure:"db"`
	PoolSize     int    `mapstructure:"pool_size"`
	MinIdleConns int    `mapstructure:"min_idle_conns"`
	DialTimeout  string `mapstructure:"dial_timeout"`
	ReadTimeout  string `mapstructure:"read_timeout"`
	WriteTimeout string `mapstructure:"write_timeout"`
}

type HTTPBackendConfig struct {
	BaseURL     string            `mapstructure:"base_url"`
	Headers     map[string]string `mapstructure:"headers"`
	Timeout     string            `mapstructure:"timeout"`
	MaxConns    int               `mapstructure:"max_conns"`
	KeyParam    string            `mapstructure:"key_param"`
	ValueParam  string            `mapstructure:"value_param"`
	TTLParam    string            `mapstructure:"ttl_param"`
}

type GRPCBackendConfig struct {
	Addr         string `mapstructure:"addr"`
	Timeout      string `mapstructure:"timeout"`
	MaxMsgSize   int    `mapstructure:"max_msg_size"`
	UseTLS       bool   `mapstructure:"use_tls"`
	CertFile     string `mapstructure:"cert_file"`
}

type RoutingRule struct {
	Strategy      RoutingStrategy `mapstructure:"strategy"`
	Pattern       string          `mapstructure:"pattern"`
	BackendID     string          `mapstructure:"backend_id"`
	Priority      int             `mapstructure:"priority"`
	HashMod       int             `mapstructure:"hash_mod"`
	HashRemainder int             `mapstructure:"hash_remainder"`
}

type BackendConfig struct {
	ID             string              `mapstructure:"id"`
	Type           BackendType         `mapstructure:"type"`
	Name           string              `mapstructure:"name"`
	Weight         int                 `mapstructure:"weight"`
	Group          string              `mapstructure:"group"`
	Memory         *MemoryBackendConfig `mapstructure:"memory,omitempty"`
	Redis          *RedisBackendConfig  `mapstructure:"redis,omitempty"`
	HTTP           *HTTPBackendConfig   `mapstructure:"http,omitempty"`
	GRPC           *GRPCBackendConfig   `mapstructure:"grpc,omitempty"`
	HealthCheck    *HealthCheckConfig   `mapstructure:"health_check,omitempty"`
	CircuitBreaker *CircuitBreakerConfig `mapstructure:"circuit_breaker,omitempty"`
}

type CacheConfig struct {
	DefaultMode      ReadWriteMode `mapstructure:"default_mode"`
	DefaultTTL       string        `mapstructure:"default_ttl"`
	MaxValueSize     int64         `mapstructure:"max_value_size"`
	EnableVersioning bool          `mapstructure:"enable_versioning"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

type ServerConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	ReadTimeout  string `mapstructure:"read_timeout"`
	WriteTimeout string `mapstructure:"write_timeout"`
	IdleTimeout  string `mapstructure:"idle_timeout"`
}

type AppConfig struct {
	Server    ServerConfig      `mapstructure:"server"`
	Cache     CacheConfig       `mapstructure:"cache"`
	Metrics   MetricsConfig     `mapstructure:"metrics"`
	Backends  []BackendConfig   `mapstructure:"backends"`
	Routing   []RoutingRule     `mapstructure:"routing"`
	PubSub    PubSubConfig      `mapstructure:"pubsub"`
	Gossip    GossipConfig      `mapstructure:"gossip"`
	LogLevel  string            `mapstructure:"log_level"`
	LogFormat string            `mapstructure:"log_format"`
}

var (
	instance *AppConfig
	once     sync.Once
	mu       sync.RWMutex
)

func Load(configPath string) (*AppConfig, error) {
	var err error
	once.Do(func() {
		instance = &AppConfig{}
		err = loadConfig(configPath, instance)
		if err != nil {
			return
		}
		setupHotReload(configPath)
	})
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func Get() *AppConfig {
	mu.RLock()
	defer mu.RUnlock()
	return instance
}

func loadConfig(configPath string, cfg *AppConfig) error {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")
	v.AutomaticEnv()
	v.SetEnvPrefix("CACHE_PROXY")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8090)
	v.SetDefault("server.read_timeout", "30s")
	v.SetDefault("server.write_timeout", "30s")
	v.SetDefault("server.idle_timeout", "60s")
	v.SetDefault("cache.default_mode", "cache-aside")
	v.SetDefault("cache.default_ttl", "5m")
	v.SetDefault("cache.max_value_size", 1048576)
	v.SetDefault("cache.enable_versioning", true)
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("log_level", "info")
	v.SetDefault("log_format", "json")

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := v.Unmarshal(cfg); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return nil
}

func setupHotReload(configPath string) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	v.OnConfigChange(func(e fsnotify.Event) {
		log.Info().Str("file", e.Name).Msg("Config file changed, reloading")
		newCfg := &AppConfig{}
		if err := loadConfig(configPath, newCfg); err != nil {
			log.Error().Err(err).Msg("Failed to reload config")
			return
		}
		mu.Lock()
		instance = newCfg
		mu.Unlock()
		log.Info().Msg("Config reloaded successfully")
	})
	v.WatchConfig()
}

func (c *AppConfig) GetBackendByID(id string) *BackendConfig {
	for i := range c.Backends {
		if c.Backends[i].ID == id {
			return &c.Backends[i]
		}
	}
	return nil
}

func (c *AppConfig) GetBackendsByGroup(group string) []BackendConfig {
	var result []BackendConfig
	for _, b := range c.Backends {
		if b.Group == group {
			result = append(result, b)
		}
	}
	return result
}

func (c *AppConfig) GetBackendsByType(backendType BackendType) []BackendConfig {
	var result []BackendConfig
	for _, b := range c.Backends {
		if b.Type == backendType {
			result = append(result, b)
		}
	}
	return result
}

func ParseDuration(s string, defaultDur time.Duration) time.Duration {
	if s == "" {
		return defaultDur
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Warn().Str("duration", s).Err(err).Msg("Failed to parse duration, using default")
		return defaultDur
	}
	return d
}
