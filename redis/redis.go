package redis

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"sync"
)

type Redis struct {
	log         *zap.Logger
	statsd      *statsd.Client
	opts        *redis.Options
	clusterOpts *redis.ClusterOptions

	mu     sync.RWMutex
	Client redis.UniversalClient

	roundTripCtx    context.Context
	roundTripCancel func()
}

func Connect(log *zap.Logger, sd *statsd.Client, opts *redis.Options, clusterOpts *redis.ClusterOptions) (*Redis, error) {
	var c redis.UniversalClient
	if clusterOpts == nil {
		c = redis.NewClient(opts)
	} else {
		c = redis.NewClusterClient(clusterOpts)
	}

	rtCtx, rtCancel := context.WithCancel(context.Background())
	r := Redis{
		log:             log,
		statsd:          sd,
		opts:            opts,
		clusterOpts:     clusterOpts,
		Client:          c,
		roundTripCtx:    rtCtx,
		roundTripCancel: rtCancel,
	}
	return &r, nil
}

func (r *Redis) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Client == nil {
		// already closed
		return
	}
	r.roundTripCancel()
	r.log.Info("Disconnect")
	err := r.Client.Close()
	r.Client = nil
	if err != nil {
		r.log.Info("Error disconnecting", zap.Error(err))
	}
}
