package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/logger"
)

type InvalidationMessage struct {
	Key       string    `json:"key"`
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`
}

type InvalidationHandler func(key string)

type PubSub interface {
	Publish(key string) error
	Subscribe(handler InvalidationHandler) error
	Close() error
}

type RedisPubSub struct {
	mu       sync.RWMutex
	nodeID   string
	client   *goredis.Client
	channel  string
	handlers []InvalidationHandler
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewRedisPubSub(nodeID string, cfg *config.PubSubConfig) (*RedisPubSub, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	if cfg.Type != config.PubSubTypeRedis {
		return nil, nil
	}

	opt, err := goredis.ParseURL(cfg.RedisURL)
	if err != nil {
		opt = &goredis.Options{
			Addr: "localhost:6379",
		}
	}

	client := goredis.NewClient(opt)

	channel := cfg.Channel
	if channel == "" {
		channel = "cache-invalidation"
	}

	return &RedisPubSub{
		nodeID:   nodeID,
		client:   client,
		channel:  channel,
		stopChan: make(chan struct{}),
	}, nil
}

func (r *RedisPubSub) Publish(key string) error {
	if r == nil || r.client == nil {
		return nil
	}

	msg := &InvalidationMessage{
		Key:       key,
		NodeID:    r.nodeID,
		Timestamp: time.Now(),
		Type:      "invalidate",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal invalidation message: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = r.client.Publish(ctx, r.channel, data).Err()
	if err != nil {
		logger.Warn().Str("key", key).Err(err).Msg("Failed to publish invalidation message")
		return err
	}

	logger.Debug().Str("key", key).Str("channel", r.channel).Msg("Published invalidation message")
	return nil
}

func (r *RedisPubSub) Subscribe(handler InvalidationHandler) error {
	if r == nil || r.client == nil {
		return nil
	}

	r.mu.Lock()
	r.handlers = append(r.handlers, handler)
	r.mu.Unlock()

	if len(r.handlers) == 1 {
		r.startListener()
	}

	return nil
}

func (r *RedisPubSub) startListener() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		ctx := context.Background()
		pubsub := r.client.Subscribe(ctx, r.channel)
		defer pubsub.Close()

		_, err := pubsub.Receive(ctx)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to subscribe to invalidation channel")
			return
		}

		logger.Info().Str("channel", r.channel).Msg("Subscribed to invalidation channel")

		ch := pubsub.Channel()

		for {
			select {
			case <-r.stopChan:
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}

				var invMsg InvalidationMessage
				if err := json.Unmarshal([]byte(msg.Payload), &invMsg); err != nil {
					logger.Warn().Err(err).Msg("Failed to unmarshal invalidation message")
					continue
				}

				if invMsg.NodeID == r.nodeID {
					continue
				}

				logger.Debug().
					Str("key", invMsg.Key).
					Str("from_node", invMsg.NodeID).
					Msg("Received invalidation message")

				r.mu.RLock()
				handlers := make([]InvalidationHandler, len(r.handlers))
				copy(handlers, r.handlers)
				r.mu.RUnlock()

				for _, h := range handlers {
					go h(invMsg.Key)
				}
			}
		}
	}()
}

func (r *RedisPubSub) Close() error {
	if r == nil {
		return nil
	}

	close(r.stopChan)
	r.wg.Wait()

	if r.client != nil {
		return r.client.Close()
	}
	return nil
}
