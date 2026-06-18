package router

import (
	"hash/fnv"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/logger"
	"go-cache-proxy/internal/storage"
)

type Router struct {
	mu          sync.RWMutex
	backends    map[string]storage.Backend
	rules       []config.RoutingRule
	hintRoutes  map[string][]string
	blacklist   map[string][]string
	whitelist   map[string][]string
}

func New() *Router {
	return &Router{
		backends:   make(map[string]storage.Backend),
		rules:      make([]config.RoutingRule, 0),
		hintRoutes: make(map[string][]string),
		blacklist:  make(map[string][]string),
		whitelist:  make(map[string][]string),
	}
}

func (r *Router) RegisterBackend(backend storage.Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backends[backend.ID()] = backend
}

func (r *Router) UnregisterBackend(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.backends, id)
}

func (r *Router) SetRules(rules []config.RoutingRule) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sorted := make([]config.RoutingRule, len(rules))
	copy(sorted, rules)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Priority > sorted[j].Priority
	})

	r.rules = sorted

	r.hintRoutes = make(map[string][]string)
	r.blacklist = make(map[string][]string)
	r.whitelist = make(map[string][]string)

	for _, rule := range sorted {
		switch rule.Strategy {
		case config.RoutingStrategyBlacklist:
			r.blacklist[rule.Pattern] = append(r.blacklist[rule.Pattern], rule.BackendID)
		case config.RoutingStrategyWhitelist:
			r.whitelist[rule.Pattern] = append(r.whitelist[rule.Pattern], rule.BackendID)
		}
	}
}

func (r *Router) SetHintRoute(hint string, backendIDs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hintRoutes[hint] = backendIDs
}

func (r *Router) GetBackend(key string, hint string) (storage.Backend, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if hint != "" {
		if backendIDs, ok := r.hintRoutes[hint]; ok && len(backendIDs) > 0 {
			backendID := backendIDs[0]
			if backend, exists := r.backends[backendID]; exists && backend.Healthy() {
				return backend, nil
			}
			for _, id := range backendIDs[1:] {
				if backend, exists := r.backends[id]; exists && backend.Healthy() {
					return backend, nil
				}
			}
		}
	}

	for _, rule := range r.rules {
		if r.matchRule(rule, key) {
			if rule.Strategy == config.RoutingStrategyBlacklist {
				continue
			}
			if rule.Strategy == config.RoutingStrategyWhitelist {
				if backend, exists := r.backends[rule.BackendID]; exists && backend.Healthy() {
					return backend, nil
				}
				continue
			}
			if backend, exists := r.backends[rule.BackendID]; exists && backend.Healthy() {
				return backend, nil
			}
			logger.Warn().
				Str("rule_pattern", rule.Pattern).
				Str("backend_id", rule.BackendID).
				Msg("Matched backend is unhealthy or not found")
		}
	}

	return r.getFallbackBackend()
}

func (r *Router) GetBackendsForRead(key string, hint string) []storage.Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []storage.Backend
	seen := make(map[string]bool)

	if hint != "" {
		if backendIDs, ok := r.hintRoutes[hint]; ok {
			for _, id := range backendIDs {
				if backend, exists := r.backends[id]; exists && backend.Healthy() && !seen[id] {
					result = append(result, backend)
					seen[id] = true
				}
			}
		}
	}

	for _, rule := range r.rules {
		if r.matchRule(rule, key) {
			if rule.Strategy == config.RoutingStrategyBlacklist {
				continue
			}
			id := rule.BackendID
			if backend, exists := r.backends[id]; exists && backend.Healthy() && !seen[id] {
				result = append(result, backend)
				seen[id] = true
			}
		}
	}

	for _, backend := range r.backends {
		if backend.Healthy() && !seen[backend.ID()] {
			result = append(result, backend)
			seen[backend.ID()] = true
		}
	}

	return result
}

func (r *Router) matchRule(rule config.RoutingRule, key string) bool {
	switch rule.Strategy {
	case config.RoutingStrategyExact:
		return strings.HasPrefix(key, strings.TrimSuffix(rule.Pattern, "*"))
	case config.RoutingStrategyWildcard:
		return matchWildcard(rule.Pattern, key)
	case config.RoutingStrategyHash:
		if rule.HashMod == 0 {
			return false
		}
		hash := xxhash.Sum64String(key)
		remainder := int(hash % uint64(rule.HashMod))
		return remainder == rule.HashRemainder
	case config.RoutingStrategyBlacklist:
		return matchWildcard(rule.Pattern, key)
	case config.RoutingStrategyWhitelist:
		return matchWildcard(rule.Pattern, key)
	default:
		return false
	}
}

func (r *Router) isBlacklisted(key string, backendID string) bool {
	for pattern, backendIDs := range r.blacklist {
		if matchWildcard(pattern, key) {
			for _, id := range backendIDs {
				if id == backendID {
					return true
				}
			}
		}
	}
	return false
}

func (r *Router) isWhitelisted(key string, backendID string) bool {
	if len(r.whitelist) == 0 {
		return true
	}
	for pattern, backendIDs := range r.whitelist {
		if matchWildcard(pattern, key) {
			for _, id := range backendIDs {
				if id == backendID {
					return true
				}
			}
		}
	}
	return false
}

func (r *Router) getFallbackBackend() (storage.Backend, error) {
	var firstHealthy storage.Backend
	for _, backend := range r.backends {
		if backend.Healthy() {
			if firstHealthy == nil || backend.Weight() > firstHealthy.Weight() {
				firstHealthy = backend
			}
		}
	}
	if firstHealthy != nil {
		return firstHealthy, nil
	}
	return nil, storage.ErrBackendUnhealthy
}

func (r *Router) GetAllBackends() []storage.Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]storage.Backend, 0, len(r.backends))
	for _, backend := range r.backends {
		result = append(result, backend)
	}
	return result
}

func (r *Router) GetBackendByID(id string) storage.Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.backends[id]
}

func (r *Router) HashKey(key string, mod int) int {
	if mod <= 0 {
		mod = 1
	}
	h := fnv.New64a()
	h.Write([]byte(key))
	hash := h.Sum64()
	return int(hash % uint64(mod))
}

func matchWildcard(pattern, str string) bool {
	if pattern == str {
		return true
	}
	if strings.Contains(pattern, "*") || strings.Contains(pattern, "?") {
		matched, _ := filepath.Match(pattern, str)
		return matched
	}
	regexPattern := regexp.QuoteMeta(pattern)
	regexPattern = strings.ReplaceAll(regexPattern, "\\*", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "\\?", ".")
	regexPattern = "^" + regexPattern + "$"
	matched, _ := regexp.MatchString(regexPattern, str)
	return matched
}
