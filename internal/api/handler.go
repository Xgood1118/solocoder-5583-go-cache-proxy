package api

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go-cache-proxy/internal/cache"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/health"
	"go-cache-proxy/internal/logger"
	"go-cache-proxy/internal/metrics"
	"go-cache-proxy/internal/storage"
)

type CacheHandler struct {
	cacheService *cache.CacheService
	health       *health.HealthChecker
	metrics      *metrics.Metrics
	config       *config.AppConfig
}

type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Type    string `json:"type"`
}

type GetResponse struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version uint64 `json:"version,omitempty"`
	TTL     int64  `json:"ttl,omitempty"`
}

type SetRequest struct {
	Key     string `json:"key" binding:"required"`
	Value   string `json:"value" binding:"required"`
	Version uint64 `json:"version,omitempty"`
	TTL     int64  `json:"ttl,omitempty"`
}

type SetResponse struct {
	Key     string `json:"key"`
	Success bool   `json:"success"`
	Version uint64 `json:"version,omitempty"`
}

type DeleteResponse struct {
	Key     string `json:"key"`
	Success bool   `json:"success"`
}

type HealthResponse struct {
	Status  string                 `json:"status"`
	Backends map[string]interface{} `json:"backends,omitempty"`
}

func NewHandler(cs *cache.CacheService, hc *health.HealthChecker, m *metrics.Metrics, cfg *config.AppConfig) *CacheHandler {
	return &CacheHandler{
		cacheService: cs,
		health:       hc,
		metrics:      m,
		config:       cfg,
	}
}

func (h *CacheHandler) RegisterRoutes(router *gin.Engine) {
	api := router.Group("/api/v1")
	{
		cache := api.Group("/cache")
		{
			cache.GET("/:key", h.Get)
			cache.POST("", h.Set)
			cache.DELETE("/:key", h.Delete)
			cache.HEAD("/:key", h.Exists)
		}
		admin := api.Group("/admin")
		{
			admin.GET("/health", h.Health)
			admin.GET("/health/detail", h.HealthDetail)
			admin.GET("/config", h.GetConfig)
		}
	}

	if h.config.Metrics.Enabled {
		path := h.config.Metrics.Path
		if path == "" {
			path = "/metrics"
		}
		router.GET(path, gin.WrapH(promhttp.HandlerFor(h.metrics.Registry(), promhttp.HandlerOpts{})))
	}
}

func (h *CacheHandler) Get(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		h.error(c, http.StatusBadRequest, "routing", "key is required")
		return
	}

	version := uint64(0)
	if versionStr := c.Query("version"); versionStr != "" {
		if v, err := strconv.ParseUint(versionStr, 10, 64); err == nil {
			version = v
		}
	}

	hint := c.Query("hint")
	skipCache := c.Query("skip_cache") == "true"

	opts := &cache.GetOptions{
		Version:     version,
		RoutingHint: hint,
		SkipCache:   skipCache,
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	entry, err := h.cacheService.Get(ctx, key, opts)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			h.error(c, http.StatusNotFound, "backend", err.Error())
			return
		}
		if errors.Is(err, storage.ErrVersionMismatch) {
			h.error(c, http.StatusConflict, "consistency", err.Error())
			return
		}
		if errors.Is(err, storage.ErrBackendUnhealthy) {
			h.error(c, http.StatusServiceUnavailable, "backend", err.Error())
			return
		}
		h.error(c, http.StatusInternalServerError, "backend", err.Error())
		return
	}

	value := base64.StdEncoding.EncodeToString(entry.Value)

	c.JSON(http.StatusOK, GetResponse{
		Key:     key,
		Value:   value,
		Version: entry.Version,
		TTL:     int64(entry.TTL.Seconds()),
	})
}

func (h *CacheHandler) Set(c *gin.Context) {
	var req SetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.error(c, http.StatusBadRequest, "routing", "invalid request body: "+err.Error())
		return
	}

	valueBytes, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		h.error(c, http.StatusBadRequest, "routing", "invalid value encoding, expected base64")
		return
	}

	hint := c.Query("hint")
	mode := c.Query("mode")
	checkVersion := c.Query("check_version") == "true"

	var ttl time.Duration
	if req.TTL > 0 {
		ttl = time.Duration(req.TTL) * time.Second
	}

	entry := &storage.Entry{
		Key:   req.Key,
		Value: valueBytes,
		TTL:   ttl,
	}

	opts := &cache.SetOptions{
		Version:      req.Version,
		RoutingHint:  hint,
		TTL:          ttl,
		CheckVersion: checkVersion,
	}

	if mode != "" {
		opts.Mode = config.ReadWriteMode(mode)
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	err = h.cacheService.Set(ctx, entry, opts)
	if err != nil {
		if errors.Is(err, storage.ErrVersionMismatch) {
			h.error(c, http.StatusConflict, "consistency", err.Error())
			return
		}
		if errors.Is(err, storage.ErrBackendUnhealthy) {
			h.error(c, http.StatusServiceUnavailable, "backend", err.Error())
			return
		}
		h.error(c, http.StatusInternalServerError, "backend", err.Error())
		return
	}

	c.JSON(http.StatusOK, SetResponse{
		Key:     req.Key,
		Success: true,
		Version: entry.Version,
	})
}

func (h *CacheHandler) Delete(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		h.error(c, http.StatusBadRequest, "routing", "key is required")
		return
	}

	hint := c.Query("hint")

	opts := &cache.DeleteOptions{
		RoutingHint: hint,
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	err := h.cacheService.Delete(ctx, key, opts)
	if err != nil {
		if errors.Is(err, storage.ErrBackendUnhealthy) {
			h.error(c, http.StatusServiceUnavailable, "backend", err.Error())
			return
		}
		h.error(c, http.StatusInternalServerError, "backend", err.Error())
		return
	}

	c.JSON(http.StatusOK, DeleteResponse{
		Key:     key,
		Success: true,
	})
}

func (h *CacheHandler) Exists(c *gin.Context) {
	key := c.Param("key")
	if key == "" {
		c.Status(http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	backends := h.config.GetBackendsByType(config.BackendTypeMemory)
	var exists bool
	var err error

	if len(backends) > 0 {
		for _, backend := range backends {
			backendInst := h.getBackendByID(backend.ID)
			if backendInst != nil {
				exists, err = backendInst.Exists(ctx, key)
				if err == nil && exists {
					c.Status(http.StatusOK)
					return
				}
			}
		}
	}

	if exists {
		c.Status(http.StatusOK)
	} else {
		c.Status(http.StatusNotFound)
	}
}

func (h *CacheHandler) Health(c *gin.Context) {
	status := "healthy"
	healthStatus := h.health.GetAllHealthStatus()

	for _, s := range healthStatus {
		if !s.Healthy {
			status = "degraded"
			break
		}
	}

	c.JSON(http.StatusOK, HealthResponse{
		Status: status,
	})
}

func (h *CacheHandler) HealthDetail(c *gin.Context) {
	status := "healthy"
	healthStatus := h.health.GetAllHealthStatus()

	backends := make(map[string]interface{})
	for id, s := range healthStatus {
		if !s.Healthy {
			status = "degraded"
		}
		backends[id] = map[string]interface{}{
			"healthy":              s.Healthy,
			"last_check":           s.LastCheck,
			"success_count":        s.SuccessCount,
			"failure_count":        s.FailureCount,
			"consecutive_success":  s.ConsecutiveSuccess,
			"consecutive_failure":  s.ConsecutiveFailure,
		}
	}

	c.JSON(http.StatusOK, HealthResponse{
		Status:   status,
		Backends: backends,
	})
}

func (h *CacheHandler) GetConfig(c *gin.Context) {
	c.JSON(http.StatusOK, h.config)
}

func (h *CacheHandler) error(c *gin.Context, code int, errType string, message string) {
	logger.Warn().
		Int("code", code).
		Str("type", errType).
		Str("path", c.Request.URL.Path).
		Msg(message)

	c.JSON(code, ErrorResponse{
		Code:    code,
		Message: message,
		Type:    errType,
	})
}

func (h *CacheHandler) getBackendByID(id string) storage.Backend {
	return nil
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Routing-Hint")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		latency := time.Since(start)
		statusCode := c.Writer.Status()
		clientIP := c.ClientIP()

		logger.Info().
			Str("method", method).
			Str("path", path).
			Int("status", statusCode).
			Str("client_ip", clientIP).
			Str("latency", latency.String()).
			Msg("HTTP request")
	}
}

func GetRoutingHint() gin.HandlerFunc {
	return func(c *gin.Context) {
		hint := c.GetHeader("X-Routing-Hint")
		if hint != "" {
			c.Set("routing_hint", hint)
			q := c.Request.URL.Query()
			q.Set("hint", hint)
			c.Request.URL.RawQuery = q.Encode()
		}
		c.Next()
	}
}
