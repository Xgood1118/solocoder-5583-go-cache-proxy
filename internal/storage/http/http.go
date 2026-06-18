package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	neturl "net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/storage"
)

type HTTPBackend struct {
	id         string
	name       string
	weight     int
	healthy    bool
	mu         sync.RWMutex
	client     *fasthttp.Client
	baseURL    string
	headers    map[string]string
	keyParam   string
	valueParam string
	ttlParam   string
	timeout    time.Duration
	defaultTTL time.Duration
}

func New(id string, name string, weight int, cfg *config.HTTPBackendConfig) (*HTTPBackend, error) {
	if cfg == nil {
		return nil, fmt.Errorf("http config is required")
	}

	baseURL := cfg.BaseURL
	if baseURL == "" {
		return nil, fmt.Errorf("base_url is required for http backend")
	}
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}

	timeout := config.ParseDuration(cfg.Timeout, 10*time.Second)
	maxConns := cfg.MaxConns
	if maxConns == 0 {
		maxConns = 100
	}

	keyParam := cfg.KeyParam
	if keyParam == "" {
		keyParam = "key"
	}
	valueParam := cfg.ValueParam
	if valueParam == "" {
		valueParam = "value"
	}
	ttlParam := cfg.TTLParam
	if ttlParam == "" {
		ttlParam = "ttl"
	}

	client := &fasthttp.Client{
		Name:                fmt.Sprintf("cache-proxy-%s", id),
		MaxConnsPerHost:     maxConns,
		ReadTimeout:         timeout,
		WriteTimeout:        timeout,
		MaxIdleConnDuration: 30 * time.Second,
		MaxConnDuration:     10 * time.Minute,
	}

	return &HTTPBackend{
		id:         id,
		name:       name,
		weight:     weight,
		healthy:    true,
		client:     client,
		baseURL:    baseURL,
		headers:    cfg.Headers,
		keyParam:   keyParam,
		valueParam: valueParam,
		ttlParam:   ttlParam,
		timeout:    timeout,
		defaultTTL: 5 * time.Minute,
	}, nil
}

func (h *HTTPBackend) ID() string {
	return h.id
}

func (h *HTTPBackend) Type() string {
	return string(config.BackendTypeHTTP)
}

func (h *HTTPBackend) Name() string {
	return h.name
}

func (h *HTTPBackend) Weight() int {
	return h.weight
}

func (h *HTTPBackend) Healthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.healthy
}

func (h *HTTPBackend) SetHealthy(healthy bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthy = healthy
}

func (h *HTTPBackend) Get(ctx context.Context, key string) (*storage.Entry, error) {
	if !h.Healthy() {
		return nil, storage.ErrBackendUnhealthy
	}

	url := fmt.Sprintf("%sget?%s=%s", h.baseURL, h.keyParam, neturl.QueryEscape(key))

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodGet)
	h.addHeaders(req)

	reqCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	err := h.client.DoTimeout(req, resp, h.timeout)
	if err != nil {
		select {
		case <-reqCtx.Done():
			return nil, fmt.Errorf("request timeout for key %s: %w", key, err)
		default:
			return nil, fmt.Errorf("request failed for key %s: %w", key, err)
		}
	}

	statusCode := resp.StatusCode()
	if statusCode == fasthttp.StatusNotFound {
		return nil, storage.ErrKeyNotFound
	}
	if statusCode != fasthttp.StatusOK {
		return nil, fmt.Errorf("request failed with status %d for key %s: %s", statusCode, key, string(resp.Body()))
	}

	body := resp.Body()
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return &storage.Entry{
			Key:   key,
			Value: body,
		}, nil
	}

	value, _ := result["value"].(string)
	version, _ := result["version"].(float64)

	return &storage.Entry{
		Key:     key,
		Value:   []byte(value),
		Version: uint64(version),
	}, nil
}

func (h *HTTPBackend) Set(ctx context.Context, entry *storage.Entry) error {
	if !h.Healthy() {
		return storage.ErrBackendUnhealthy
	}

	ttl := entry.TTL
	if ttl <= 0 {
		ttl = h.defaultTTL
	}

	url := fmt.Sprintf("%sset", h.baseURL)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/x-www-form-urlencoded")
	h.addHeaders(req)

	body := fmt.Sprintf("%s=%s&%s=%s&%s=%d",
		h.keyParam, neturl.QueryEscape(entry.Key),
		h.valueParam, neturl.QueryEscape(string(entry.Value)),
		h.ttlParam, int(ttl.Seconds()),
	)
	req.SetBodyString(body)

	err := h.client.DoTimeout(req, resp, h.timeout)
	if err != nil {
		return fmt.Errorf("set request failed for key %s: %w", entry.Key, err)
	}

	statusCode := resp.StatusCode()
	if statusCode != fasthttp.StatusOK && statusCode != fasthttp.StatusCreated {
		return fmt.Errorf("set request failed with status %d for key %s: %s", statusCode, entry.Key, string(resp.Body()))
	}

	return nil
}

func (h *HTTPBackend) Delete(ctx context.Context, key string) error {
	if !h.Healthy() {
		return storage.ErrBackendUnhealthy
	}

	url := fmt.Sprintf("%sdelete?%s=%s", h.baseURL, h.keyParam, neturl.QueryEscape(key))

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodDelete)
	h.addHeaders(req)

	err := h.client.DoTimeout(req, resp, h.timeout)
	if err != nil {
		return fmt.Errorf("delete request failed for key %s: %w", key, err)
	}

	statusCode := resp.StatusCode()
	if statusCode != fasthttp.StatusOK && statusCode != fasthttp.StatusNoContent {
		return fmt.Errorf("delete request failed with status %d for key %s: %s", statusCode, key, string(resp.Body()))
	}

	return nil
}

func (h *HTTPBackend) Exists(ctx context.Context, key string) (bool, error) {
	if !h.Healthy() {
		return false, storage.ErrBackendUnhealthy
	}

	url := fmt.Sprintf("%sexists?%s=%s", h.baseURL, h.keyParam, neturl.QueryEscape(key))

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodHead)
	h.addHeaders(req)

	err := h.client.DoTimeout(req, resp, h.timeout)
	if err != nil {
		return false, fmt.Errorf("exists request failed for key %s: %w", key, err)
	}

	statusCode := resp.StatusCode()
	if statusCode == fasthttp.StatusNotFound {
		return false, nil
	}
	if statusCode == fasthttp.StatusOK {
		return true, nil
	}
	return false, fmt.Errorf("exists request failed with status %d for key %s", statusCode, key)
}

func (h *HTTPBackend) Ping(ctx context.Context) error {
	url := h.baseURL + "health"

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodGet)
	h.addHeaders(req)

	err := h.client.DoTimeout(req, resp, h.timeout)
	if err != nil {
		if netErr, ok := err.(*net.OpError); ok {
			h.SetHealthy(false)
			return fmt.Errorf("tcp connect failed: %w", netErr)
		}
		h.SetHealthy(false)
		return err
	}

	statusCode := resp.StatusCode()
	if statusCode >= 200 && statusCode < 300 {
		h.SetHealthy(true)
		return nil
	}

	h.SetHealthy(false)
	return fmt.Errorf("health check failed with status %d", statusCode)
}

func (h *HTTPBackend) Close() error {
	h.client.CloseIdleConnections()
	return nil
}

func (h *HTTPBackend) addHeaders(req *fasthttp.Request) {
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}
}

func (h *HTTPBackend) GetBaseURL() string {
	return h.baseURL
}

func parseInt(s string, defaultVal int64) int64 {
	if s == "" {
		return defaultVal
	}
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultVal
	}
	return val
}
