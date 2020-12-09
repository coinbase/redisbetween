package handlers

import (
	"context"
	"fmt"
	"github.cbhq.net/engineering/memcachedbetween/pool"
	"github.cbhq.net/engineering/redisbetween/config"
	"github.cbhq.net/engineering/redisbetween/redis"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

type connection struct {
	log    *zap.Logger
	statsd *statsd.Client
	cfg    *config.Config

	ctx         context.Context
	conn        net.Conn
	address     string
	id          uint64
	server      *pool.Server
	kill        chan interface{}
	interceptor MessageInterceptor
}
type MessageInterceptor func(incomingCmd string, m *redis.Message)

func CommandConnection(log *zap.Logger, sd *statsd.Client, cfg *config.Config, conn net.Conn, address string, id uint64, server *pool.Server, kill chan interface{}, interceptor MessageInterceptor) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:         log,
		statsd:      sd,
		cfg:         cfg,
		ctx:         context.Background(),
		conn:        conn,
		address:     address,
		id:          id,
		server:      server,
		kill:        kill,
		interceptor: interceptor,
	}
	c.processMessages()
}

func (c *connection) processMessages() {
	for {
		l, err := c.handleMessage()
		if err != nil {
			if err != io.EOF {
				select {
				case <-c.kill:
					// ignore errors from force shutdown
				default:
					l.Error("Error handling message", zap.Error(err))
				}
			}
			return
		}
	}
}

func (c *connection) handleMessage() (*zap.Logger, error) {
	var err error

	defer func(start time.Time) {
		_ = c.statsd.Timing("handle_message", time.Since(start), []string{
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

	l := c.log

	var wm *redis.Message
	if wm, err = ReadWireMessage(c.ctx, l, c.conn, c.address, c.id, 0, c.conn.Close); err != nil {
		return l, err
	}

	var incomingCmd string
	if wm.IsArray() {
		incomingCmd = strings.ToUpper(string(wm.Array[0].Value))

		if _, ok := UnsupportedCommands[incomingCmd]; ok {
			em := redis.NewError([]byte(fmt.Sprintf("redisbetween: %v is unsupported", incomingCmd)))
			err = WriteWireMessage(c.ctx, l, em, c.conn, c.address, c.id, 0, c.conn.Close)
			c.log.Debug("unsupported command", zap.String("command", incomingCmd))
			return l, err
		}

		if incomingCmd == "CLUSTER" && len(wm.Array) > 1 {
			// we only need to parse the next element if this is a CLUSTER command, for the
			// CLUSTER SLOTS and CLUSTER NODES cases
			incomingCmd += " " + strings.ToUpper(string(wm.Array[1].Value))
		}
	}

	if wm, l, err = c.roundTrip(wm); err != nil {
		return l, err
	}

	c.interceptor(incomingCmd, wm)

	err = WriteWireMessage(c.ctx, l, wm, c.conn, c.address, c.id, 0, c.conn.Close)
	return l, err
}

func (c *connection) roundTrip(wm *redis.Message) (*redis.Message, *zap.Logger, error) {
	l := c.log
	var err error

	var conn *pool.Connection
	if conn, err = c.checkoutConnection(); err != nil {
		return nil, l, err
	}
	defer func() {
		_ = conn.Return()
	}()

	l = c.log.With(zap.Uint64("upstream_id", conn.ID()))
	log.Debug("Connection checked out")

	if err = WriteWireMessage(c.ctx, l, wm, conn.Conn(), conn.Address().String(), conn.ID(), c.cfg.WriteTimeout, conn.Close); err != nil {
		return nil, l, err
	}

	res, err := ReadWireMessage(c.ctx, l, conn.Conn(), conn.Address().String(), conn.ID(), c.cfg.ReadTimeout, conn.Close)
	return res, l, err
}

func (c *connection) checkoutConnection() (conn *pool.Connection, err error) {
	defer func(start time.Time) {
		addr := ""
		if conn != nil {
			addr = conn.Address().String()
		}
		_ = c.statsd.Timing("checkout_connection", time.Since(start), []string{
			fmt.Sprintf("address:%s", addr),
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

	conn, err = c.server.Connection(c.ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func WriteWireMessage(ctx context.Context, log *zap.Logger, wm *redis.Message, nc net.Conn, address string, id uint64, writeTimeout time.Duration, close func() error) error {
	var err error
	select {
	case <-ctx.Done():
		return pool.ConnectionError{Address: address, ID: id, Wrapped: ctx.Err(), Message: "failed to write"}
	default:
	}

	var deadline time.Time
	if writeTimeout != 0 {
		deadline = time.Now().Add(writeTimeout)
	}

	if dl, ok := ctx.Deadline(); ok && (deadline.IsZero() || dl.Before(deadline)) {
		deadline = dl
	}

	if err := nc.SetWriteDeadline(deadline); err != nil {
		return pool.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "failed to set write deadline"}
	}

	err = redis.Encode(nc, wm)

	if err != nil {
		_ = close()
		return pool.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "unable to write wire message to network"}
	}
	log.Debug("Write", zap.String("address", address))
	return nil
}

func ReadWireMessage(ctx context.Context, log *zap.Logger, nc net.Conn, address string, id uint64, readTimeout time.Duration, close func() error) (*redis.Message, error) {
	select {
	case <-ctx.Done():
		// We closeConnection the connection because we don't know if there is an unread message on the wire.
		_ = close()
		return nil, pool.ConnectionError{Address: address, ID: id, Wrapped: ctx.Err(), Message: "failed to read"}
	default:
	}

	var deadline time.Time
	if readTimeout != 0 {
		deadline = time.Now().Add(readTimeout)
	}

	if dl, ok := ctx.Deadline(); ok && (deadline.IsZero() || dl.Before(deadline)) {
		deadline = dl
	}

	if err := nc.SetReadDeadline(deadline); err != nil {
		return nil, pool.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "failed to set read deadline"}
	}

	return redis.Decode(nc)
}
