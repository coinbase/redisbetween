// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package redis

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
)

var globalConnectionID uint64 = 1

func NextConnectionID() uint64 { return atomic.AddUint64(&globalConnectionID, 1) }

type connection struct {
	id                   uint64
	nc                   net.Conn // When nil, the connection is closed.
	addr                 Address
	connected            int32 // must be accessed using the sync/atomic package
	connectDone          chan struct{}
	connectErr           error
	config               *connectionConfig
	cancelConnectContext context.CancelFunc
	connectContextMade   chan struct{}
	connectContextMutex  sync.Mutex

	// pool related fields
	pool         *pool
	poolID       uint64
	generation   uint64
	expireReason string
}

// newConnection handles the creation of a connection. It does not connect the connection.
func newConnection(addr Address, opts ...ConnectionOption) (*connection, error) {
	cfg, err := newConnectionConfig(opts...)
	if err != nil {
		return nil, err
	}

	c := &connection{
		id:                 NextConnectionID(),
		addr:               addr,
		connectDone:        make(chan struct{}),
		config:             cfg,
		connectContextMade: make(chan struct{}),
	}
	atomic.StoreInt32(&c.connected, initialized)

	return c, nil
}

func (c *connection) processInitializationError(err error) {
	atomic.StoreInt32(&c.connected, disconnected)
	if c.nc != nil {
		_ = c.nc.Close()
	}

	c.connectErr = ConnectionError{Address: c.addr.String(), ID: c.id, Wrapped: err}
	if c.config.errorHandlingCallback != nil {
		c.config.errorHandlingCallback(c.connectErr)
	}
}

// connect handles the I/O for a connection. It will dial and perform
// initialization handshakes.
func (c *connection) connect(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&c.connected, initialized, connected) {
		return
	}
	defer close(c.connectDone)

	c.connectContextMutex.Lock()
	ctx, c.cancelConnectContext = context.WithCancel(ctx)
	c.connectContextMutex.Unlock()

	defer func() {
		var cancelFn context.CancelFunc

		c.connectContextMutex.Lock()
		cancelFn = c.cancelConnectContext
		c.cancelConnectContext = nil
		c.connectContextMutex.Unlock()

		if cancelFn != nil {
			cancelFn()
		}
	}()

	close(c.connectContextMade)

	// Assign the result of DialContext to a temporary net.Conn to ensure that c.nc is not set in an error case.
	var err error
	var tempNc net.Conn
	tempNc, err = c.config.dialer.DialContext(ctx, c.addr.Network(), c.addr.String())
	if err != nil {
		c.processInitializationError(err)
		return
	}
	c.nc = tempNc
}

func (c *connection) wait() error {
	if c.connectDone != nil {
		<-c.connectDone
	}
	return c.connectErr
}

func (c *connection) closeConnectContext() {
	<-c.connectContextMade
	var cancelFn context.CancelFunc

	c.connectContextMutex.Lock()
	cancelFn = c.cancelConnectContext
	c.cancelConnectContext = nil
	c.connectContextMutex.Unlock()

	if cancelFn != nil {
		cancelFn()
	}
}

func (c *connection) close() error {
	if !atomic.CompareAndSwapInt32(&c.connected, connected, disconnected) {
		return nil
	}

	var err error
	if c.nc != nil {
		err = c.nc.Close()
	}

	return err
}

func (c *connection) closed() bool {
	return atomic.LoadInt32(&c.connected) == disconnected
}

// Connection implements the driver.Connection interface to allow reading and writing wire
// messages and the driver.Expirable interface to allow expiring.
type Connection struct {
	*connection
}

func (c *Connection) Conn() net.Conn {
	return c.connection.nc
}

func (c *Connection) Close() error {
	return c.close()
}

// Close returns this connection to the connection pool. This method may not closeConnection the underlying
// socket.
func (c *Connection) Return() error {
	if c.connection == nil {
		return nil
	}

	err := c.pool.put(c.connection)
	c.connection = nil
	return err
}

// Alive returns if the connection is still alive.
func (c *Connection) Alive() bool {
	return c.connection != nil
}

// ID returns the ID of this connection.
func (c *Connection) ID() uint64 {
	return c.id
}

// Address returns the address of this connection.
func (c *Connection) Address() Address {
	if c.connection == nil {
		return Address("0.0.0.0")
	}
	return c.addr
}

// LocalAddress returns the local address of the connection
func (c *Connection) LocalAddress() Address {
	if c.connection == nil || c.nc == nil {
		return Address("0.0.0.0")
	}
	return Address(c.nc.LocalAddr().String())
}
