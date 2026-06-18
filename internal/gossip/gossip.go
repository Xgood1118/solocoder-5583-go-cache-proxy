package gossip

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/logger"
)

type InvalidationMessage struct {
	Key       string    `json:"key"`
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
}

type InvalidationHandler func(key string)

type GossipProtocol struct {
	mu        sync.RWMutex
	nodeID    string
	config    *config.GossipConfig
	memberlist *memberlist.Memberlist
	handlers  []InvalidationHandler
	delegate  *gossipDelegate
	stopChan  chan struct{}
}

type gossipDelegate struct {
	gossip *GossipProtocol
	nodeID string
}

func (d *gossipDelegate) NodeMeta(limit int) []byte {
	meta := map[string]string{
		"node_id": d.nodeID,
	}
	data, _ := json.Marshal(meta)
	if len(data) > limit {
		return data[:limit]
	}
	return data
}

func (d *gossipDelegate) NotifyMsg(msg []byte) {
	var invMsg InvalidationMessage
	if err := json.Unmarshal(msg, &invMsg); err != nil {
		return
	}

	if invMsg.NodeID == d.nodeID {
		return
	}

	logger.Debug().
		Str("key", invMsg.Key).
		Str("from_node", invMsg.NodeID).
		Msg("Received gossip invalidation message")

	d.gossip.mu.RLock()
	handlers := make([]InvalidationHandler, len(d.gossip.handlers))
	copy(handlers, d.gossip.handlers)
	d.gossip.mu.RUnlock()

	for _, h := range handlers {
		go h(invMsg.Key)
	}
}

func (d *gossipDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d *gossipDelegate) LocalState(join bool) []byte {
	return nil
}

func (d *gossipDelegate) MergeRemoteState(buf []byte, join bool) {
}

type eventDelegate struct {
	gossip *GossipProtocol
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	logger.Info().
		Str("node", node.Name).
		Str("addr", node.Address()).
		Msg("Node joined cluster")
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	logger.Warn().
		Str("node", node.Name).
		Str("addr", node.Address()).
		Msg("Node left cluster")
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	logger.Info().
		Str("node", node.Name).
		Str("addr", node.Address()).
		Msg("Node updated")
}

func New(nodeID string, cfg *config.GossipConfig) (*GossipProtocol, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	bindAddr := cfg.BindAddr
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	bindPort := cfg.BindPort
	if bindPort == 0 {
		bindPort = 7946
	}

	probeInterval := config.ParseDuration(cfg.ProbeInterval, 1*time.Second)
	probeTimeout := config.ParseDuration(cfg.ProbeTimeout, 500*time.Millisecond)

	gossip := &GossipProtocol{
		nodeID:   nodeID,
		config:   cfg,
		stopChan: make(chan struct{}),
	}

	delegate := &gossipDelegate{
		gossip: gossip,
		nodeID: nodeID,
	}
	gossip.delegate = delegate

	mlConfig := memberlist.DefaultLANConfig()
	mlConfig.Name = nodeID
	mlConfig.BindAddr = bindAddr
	mlConfig.BindPort = bindPort
	mlConfig.ProbeInterval = probeInterval
	mlConfig.ProbeTimeout = probeTimeout
	mlConfig.Delegate = delegate
	mlConfig.Events = &eventDelegate{gossip: gossip}
	mlConfig.LogOutput = &loggerWriter{}

	ml, err := memberlist.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	gossip.memberlist = ml

	if len(cfg.SeedNodes) > 0 {
		_, err := ml.Join(cfg.SeedNodes)
		if err != nil {
			logger.Warn().Err(err).Msg("Failed to join seed nodes, continuing alone")
		} else {
			logger.Info().Strs("seed_nodes", cfg.SeedNodes).Msg("Joined cluster")
		}
	}

	logger.Info().
		Str("node_id", nodeID).
		Str("bind_addr", bindAddr).
		Int("bind_port", bindPort).
		Msg("Gossip protocol started")

	return gossip, nil
}

func (g *GossipProtocol) Publish(key string) error {
	if g == nil || g.memberlist == nil {
		return nil
	}

	msg := &InvalidationMessage{
		Key:       key,
		NodeID:    g.nodeID,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal gossip message: %w", err)
	}

	g.memberlist.SendToAll(data)
	logger.Debug().Str("key", key).Msg("Broadcasted gossip invalidation message")

	return nil
}

func (g *GossipProtocol) Subscribe(handler InvalidationHandler) error {
	if g == nil {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	g.handlers = append(g.handlers, handler)
	return nil
}

func (g *GossipProtocol) Close() error {
	if g == nil {
		return nil
	}

	close(g.stopChan)
	if g.memberlist != nil {
		return g.memberlist.Leave(5 * time.Second)
	}
	return nil
}

func (g *GossipProtocol) Members() []string {
	if g == nil || g.memberlist == nil {
		return nil
	}

	members := g.memberlist.Members()
	result := make([]string, 0, len(members))
	for _, m := range members {
		result = append(result, fmt.Sprintf("%s (%s)", m.Name, m.Address()))
	}
	return result
}

type loggerWriter struct{}

func (w *loggerWriter) Write(p []byte) (n int, err error) {
	logger.Debug().Str("component", "memberlist").Msg(string(p))
	return len(p), nil
}
