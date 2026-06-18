package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"go-cache-proxy/internal/config"
	"go-cache-proxy/internal/storage"
)

type GRPCBackend struct {
	id         string
	name       string
	weight     int
	healthy    bool
	mu         sync.RWMutex
	conn       *grpc.ClientConn
	client     KVServiceClient
	timeout    time.Duration
	defaultTTL time.Duration
}

type GetRequest struct {
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

type GetResponse struct {
	Value   []byte `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	Version uint64 `protobuf:"varint,2,opt,name=version,proto3" json:"version,omitempty"`
	Found   bool   `protobuf:"varint,3,opt,name=found,proto3" json:"found,omitempty"`
}

type SetRequest struct {
	Key   string        `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte        `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Ttl   time.Duration `protobuf:"varint,3,opt,name=ttl,proto3" json:"ttl,omitempty"`
}

type SetResponse struct {
	Success bool `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
}

type DeleteRequest struct {
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

type DeleteResponse struct {
	Success bool `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
}

type ExistsRequest struct {
	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

type ExistsResponse struct {
	Exists bool `protobuf:"varint,1,opt,name=exists,proto3" json:"exists,omitempty"`
}

type PingRequest struct{}

type PingResponse struct {
	Ok bool `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
}

type KVServiceClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error)
	Exists(ctx context.Context, in *ExistsRequest, opts ...grpc.CallOption) (*ExistsResponse, error)
	Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error)
}

type kvServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewKVServiceClient(cc *grpc.ClientConn) KVServiceClient {
	return &kvServiceClient{cc}
}

func (c *kvServiceClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/kv.KVService/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvServiceClient) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error) {
	out := new(SetResponse)
	err := c.cc.Invoke(ctx, "/kv.KVService/Set", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvServiceClient) Delete(ctx context.Context, in *DeleteRequest, opts ...grpc.CallOption) (*DeleteResponse, error) {
	out := new(DeleteResponse)
	err := c.cc.Invoke(ctx, "/kv.KVService/Delete", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvServiceClient) Exists(ctx context.Context, in *ExistsRequest, opts ...grpc.CallOption) (*ExistsResponse, error) {
	out := new(ExistsResponse)
	err := c.cc.Invoke(ctx, "/kv.KVService/Exists", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kvServiceClient) Ping(ctx context.Context, in *PingRequest, opts ...grpc.CallOption) (*PingResponse, error) {
	out := new(PingResponse)
	err := c.cc.Invoke(ctx, "/kv.KVService/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func New(id string, name string, weight int, cfg *config.GRPCBackendConfig) (*GRPCBackend, error) {
	if cfg == nil {
		return nil, fmt.Errorf("grpc config is required")
	}

	addr := cfg.Addr
	if addr == "" {
		return nil, fmt.Errorf("addr is required for grpc backend")
	}

	timeout := config.ParseDuration(cfg.Timeout, 10*time.Second)
	maxMsgSize := cfg.MaxMsgSize
	if maxMsgSize == 0 {
		maxMsgSize = 4 * 1024 * 1024
	}

	var dialOpts []grpc.DialOption
	if cfg.UseTLS && cfg.CertFile != "" {
		creds, err := credentials.NewClientTLSFromFile(cfg.CertFile, "")
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	dialOpts = append(dialOpts,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
		grpc.WithBlock(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to grpc server: %w", err)
	}

	client := NewKVServiceClient(conn)

	return &GRPCBackend{
		id:         id,
		name:       name,
		weight:     weight,
		healthy:    true,
		conn:       conn,
		client:     client,
		timeout:    timeout,
		defaultTTL: 5 * time.Minute,
	}, nil
}

func (g *GRPCBackend) ID() string {
	return g.id
}

func (g *GRPCBackend) Type() string {
	return string(config.BackendTypeGRPC)
}

func (g *GRPCBackend) Name() string {
	return g.name
}

func (g *GRPCBackend) Weight() int {
	return g.weight
}

func (g *GRPCBackend) Healthy() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.healthy
}

func (g *GRPCBackend) SetHealthy(healthy bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.healthy = healthy
}

func (g *GRPCBackend) Get(ctx context.Context, key string) (*storage.Entry, error) {
	if !g.Healthy() {
		return nil, storage.ErrBackendUnhealthy
	}

	reqCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	resp, err := g.client.Get(reqCtx, &GetRequest{Key: key})
	if err != nil {
		return nil, fmt.Errorf("grpc get failed for key %s: %w", key, err)
	}

	if !resp.Found {
		return nil, storage.ErrKeyNotFound
	}

	return &storage.Entry{
		Key:     key,
		Value:   resp.Value,
		Version: resp.Version,
	}, nil
}

func (g *GRPCBackend) Set(ctx context.Context, entry *storage.Entry) error {
	if !g.Healthy() {
		return storage.ErrBackendUnhealthy
	}

	ttl := entry.TTL
	if ttl <= 0 {
		ttl = g.defaultTTL
	}

	reqCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	resp, err := g.client.Set(reqCtx, &SetRequest{
		Key:   entry.Key,
		Value: entry.Value,
		Ttl:   ttl,
	})
	if err != nil {
		return fmt.Errorf("grpc set failed for key %s: %w", entry.Key, err)
	}

	if !resp.Success {
		return fmt.Errorf("grpc set failed for key %s", entry.Key)
	}

	return nil
}

func (g *GRPCBackend) Delete(ctx context.Context, key string) error {
	if !g.Healthy() {
		return storage.ErrBackendUnhealthy
	}

	reqCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	resp, err := g.client.Delete(reqCtx, &DeleteRequest{Key: key})
	if err != nil {
		return fmt.Errorf("grpc delete failed for key %s: %w", key, err)
	}

	if !resp.Success {
		return fmt.Errorf("grpc delete failed for key %s", key)
	}

	return nil
}

func (g *GRPCBackend) Exists(ctx context.Context, key string) (bool, error) {
	if !g.Healthy() {
		return false, storage.ErrBackendUnhealthy
	}

	reqCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	resp, err := g.client.Exists(reqCtx, &ExistsRequest{Key: key})
	if err != nil {
		return false, fmt.Errorf("grpc exists failed for key %s: %w", key, err)
	}

	return resp.Exists, nil
}

func (g *GRPCBackend) Ping(ctx context.Context) error {
	reqCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	_, err := g.client.Ping(reqCtx, &PingRequest{})
	if err != nil {
		if netErr, ok := err.(*net.OpError); ok {
			g.SetHealthy(false)
			return fmt.Errorf("tcp connect failed: %w", netErr)
		}
		g.SetHealthy(false)
		return err
	}

	g.SetHealthy(true)
	return nil
}

func (g *GRPCBackend) GetAddr() string {
	return ""
}

func (g *GRPCBackend) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}
