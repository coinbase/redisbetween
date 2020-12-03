// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package redis

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrServerClosed occurs when an attempt to Get a connection is made after
// the server has been closed.
var ErrServerClosed = errors.New("server is closed")

// ErrServerConnected occurs when at attempt to Connect is made after a server
// has already been connected.
var ErrServerConnected = errors.New("server is connected")

// SelectedServer represents a specific server that was selected during server selection.
// It contains the kind of the topology it was selected from.
type SelectedServer struct {
	*Server
}

// These constants represent the connection states of a server.
const (
	disconnected int32 = iota
	disconnecting
	connected
	initialized
)

// Server is a single server within a topology.
type Server struct {
	cfg             *serverConfig
	address         Address
	connectionstate int32

	// connection related fields
	pool *pool
}

// ConnectServer creates a new Server and then initializes it using the
// Connect method.
func ConnectServer(addr Address, opts ...ServerOption) (*Server, error) {
	srvr, err := NewServer(addr, opts...)
	if err != nil {
		return nil, err
	}
	err = srvr.Connect()
	if err != nil {
		return nil, err
	}
	return srvr, nil
}

// NewServer creates a new server. The mongodb server at the address will be monitored
// on an internal monitoring goroutine.
func NewServer(addr Address, opts ...ServerOption) (*Server, error) {
	cfg, err := newServerConfig(opts...)
	if err != nil {
		return nil, err
	}

	s := &Server{
		cfg:     cfg,
		address: addr,
	}

	pc := poolConfig{
		Address:     addr,
		MinPoolSize: cfg.minConns,
		MaxPoolSize: cfg.maxConns,
		PoolMonitor: cfg.poolMonitor,
	}

	connectionOpts := append(cfg.connectionOpts, withErrorHandlingCallback(s.ProcessHandshakeError))
	s.pool, err = newPool(pc, connectionOpts...)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Connect initializes the Server by starting background monitoring goroutines.
// This method must be called before a Server can be used.
func (s *Server) Connect() error {
	if !atomic.CompareAndSwapInt32(&s.connectionstate, disconnected, connected) {
		return ErrServerConnected
	}
	return s.pool.connect()
}

// Disconnect closes sockets to the server referenced by this Server.
// Subscriptions to this Server will be closed. Disconnect will shutdown
// any monitoring goroutines, closeConnection the idle connection pool, and will
// wait until all the in use connections have been returned to the connection
// pool and are closed before returning. If the context expires via
// cancellation, deadline, or timeout before the in use connections have been
// returned, the in use connections will be closed, resulting in the failure of
// any in flight read or write operations. If this method returns with no
// errors, all connections associated with this Server have been closed.
func (s *Server) Disconnect(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.connectionstate, connected, disconnecting) {
		return ErrServerClosed
	}

	err := s.pool.disconnect(ctx)
	if err != nil {
		return err
	}

	atomic.StoreInt32(&s.connectionstate, disconnected)

	return nil
}

// Connection gets a connection to the server.
func (s *Server) Connection(ctx context.Context) (*Connection, error) {
	if s.pool.monitor != nil {
		s.pool.monitor.Event(&PoolEvent{
			Type:    "ConnectionCheckOutStarted",
			Address: s.pool.address.String(),
		})
	}

	if atomic.LoadInt32(&s.connectionstate) != connected {
		return nil, ErrServerClosed
	}

	conn, err := s.pool.get(ctx)
	if err != nil {
		// The error has already been handled by connection.connect, which calls Server.ProcessHandshakeError.
		return nil, err
	}

	return &Connection{connection: conn}, nil
}

// ProcessHandshakeError implements SDAM error handling for errors that occur before a connection finishes handshaking.
func (s *Server) ProcessHandshakeError(err error) {
	if err == nil {
		return
	}
	wrappedConnErr := unwrapConnectionError(err)
	if wrappedConnErr == nil {
		return
	}

	s.pool.clear()
}

// unwrapConnectionError returns the connection error wrapped by err, or nil if err does not wrap a connection error.
func unwrapConnectionError(err error) error {
	// This is essentially an implementation of errors.As to unwrap this error until we get a ConnectionError and then
	// return ConnectionError.Wrapped.

	connErr, ok := err.(ConnectionError)
	if ok {
		return connErr.Wrapped
	}

	return nil
}
