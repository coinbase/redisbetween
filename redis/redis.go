package redis

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/mediocregopher/radix/v3"
	"go.uber.org/zap"
	"sync"
)

type Redis interface {
	Close()
	Do(cmd radix.CmdAction) error
}

type baseClient struct {
	log             *zap.Logger
	statsd          *statsd.Client
	mu              sync.RWMutex
	roundTripCtx    context.Context
	roundTripCancel func()
}

type single struct {
	baseClient
	pool *radix.Pool
}

type cluster struct {
	baseClient
	cluster *radix.Cluster
}

func Connect(log *zap.Logger, sd *statsd.Client, network string, host string, poolSize int) (Redis, error) {
	var c Redis
	rtCtx, rtCancel := context.WithCancel(context.Background())
	bc := baseClient{
		log:             log,
		statsd:          sd,
		roundTripCtx:    rtCtx,
		roundTripCancel: rtCancel,
	}
	if poolSize != 0 {
		p, err := radix.NewPool(network, host, poolSize, radix.PoolPipelineWindow(0, 0))
		if err != nil {
			return nil, err
		}
		c = &single{
			baseClient: bc,
			pool:       p,
		}
	} else {
		cl, err := radix.NewCluster([]string{host})
		if err != nil {
			return nil, err
		}
		c = &cluster{
			baseClient: bc,
			cluster:    cl,
		}
	}
	return c, nil
}

func (s *single) Do(a radix.CmdAction) error {
	return s.pool.Do(a)
}

func (s *single) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pool == nil { // already closed
		return
	}
	s.roundTripCancel()
	s.log.Info("Disconnect")
	err := s.pool.Close()
	s.pool = nil
	if err != nil {
		s.log.Info("Error disconnecting", zap.Error(err))
	}
}

func (c *cluster) Do(a radix.CmdAction) error {
	return c.cluster.Do(a)
}

func (c *cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cluster == nil { // already closed
		return
	}
	c.roundTripCancel()
	c.log.Info("Disconnect")
	err := c.cluster.Close()
	c.cluster = nil
	if err != nil {
		c.log.Info("Error disconnecting", zap.Error(err))
	}
}
