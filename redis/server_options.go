// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package redis

type serverConfig struct {
	connectionOpts []ConnectionOption
	maxConns       uint64
	minConns       uint64
	poolMonitor    *PoolMonitor
}

func newServerConfig(opts ...ServerOption) (*serverConfig, error) {
	cfg := &serverConfig{
		maxConns: 100,
	}

	for _, opt := range opts {
		err := opt(cfg)
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

// ServerOption configures a server.
type ServerOption func(*serverConfig) error

// WithConnectionOptions configures the server's connections.
func WithConnectionOptions(fn func(...ConnectionOption) []ConnectionOption) ServerOption {
	return func(cfg *serverConfig) error {
		cfg.connectionOpts = fn(cfg.connectionOpts...)
		return nil
	}
}

// WithMaxConnections configures the maximum number of connections to allow for
// a given server. If max is 0, then the default will be math.MaxInt64.
func WithMaxConnections(fn func(uint64) uint64) ServerOption {
	return func(cfg *serverConfig) error {
		cfg.maxConns = fn(cfg.maxConns)
		return nil
	}
}

// WithMinConnections configures the minimum number of connections to allow for
// a given server. If min is 0, then there is no lower limit to the number of
// connections.
func WithMinConnections(fn func(uint64) uint64) ServerOption {
	return func(cfg *serverConfig) error {
		cfg.minConns = fn(cfg.minConns)
		return nil
	}
}

// WithConnectionPoolMonitor configures the monitor for all connection pool actions
func WithConnectionPoolMonitor(fn func(*PoolMonitor) *PoolMonitor) ServerOption {
	return func(cfg *serverConfig) error {
		cfg.poolMonitor = fn(cfg.poolMonitor)
		return nil
	}
}
