package handlers

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/coinbase/memcachedbetween/pool"
	"github.com/d2army/redisbetween/messenger"
	"github.com/d2army/redisbetween/redis"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

type connection struct {
	sync.Mutex

	log          *zap.Logger
	statsd       *statsd.Client
	ctx          context.Context
	readTimeout  time.Duration
	writeTimeout time.Duration
	conn         net.Conn
	address      string
	id           uint64
	server       pool.ServerWrapper
	kill         chan interface{}
	quit         chan interface{}
	interceptor  MessageInterceptor
	messenger    messenger.Messenger
	reservations *Reservations
	isClosed     bool
}

type MessageInterceptor func(incomingCmds []string, m []*redis.Message)

func CommandConnection(log *zap.Logger, sd *statsd.Client, conn net.Conn, address string, readTimeout, writeTimeout time.Duration, id uint64, server *pool.Server, kill chan interface{}, quit chan interface{}, interceptor MessageInterceptor, reservations *Reservations) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:          log,
		statsd:       sd,
		ctx:          context.Background(),
		conn:         conn,
		address:      address,
		id:           id,
		server:       server,
		kill:         kill,
		quit:         quit,
		interceptor:  interceptor,
		messenger:    messenger.WireMessenger{},
		reservations: reservations,
	}
	c.processMessages()
}

func (c *connection) processMessages() {
	for {
		l, err := c.handleMessage()
		if err != nil {
			if err == io.EOF {
				c.isClosed = true
			} else {
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
	var incomingCmds []string

	defer func(start time.Time) {
		if len(incomingCmds) == 0 {
			incomingCmds = []string{"PARSERROR"}
		}

		_ = c.statsd.Timing("handle_message", time.Since(start), []string{
			fmt.Sprintf("success:%v", err == nil),
			fmt.Sprintf("command:%v", incomingCmds[0]),
		}, 1)
	}(time.Now())

	l := c.log

	var wm []*redis.Message
	if wm, err = c.messenger.Read(c.ctx, l, c.conn, c.address, c.id, 0, 1, true, c.conn.Close); err != nil {
		return l, err
	}

	incomingCmds, err = c.validateCommands(wm)
	if err != nil {
		mm := []*redis.Message{redis.NewError([]byte(fmt.Sprintf("redisbetween: %v", err.Error())))}
		c.log.Debug("invalid commands", zap.Strings("commands", incomingCmds), zap.Error(err))
		err = c.messenger.Write(c.ctx, l, mm, c.conn, c.address, c.id, 0, false, c.conn.Close)
		return l, err
	}

	if isSubscriptionCommand(incomingCmds) {
		if err = handleSubscription(c, wm); err != nil {
			return l, err
		}

	} else if isBlockingCommand(incomingCmds) {
		if err = handleBlocker(c, wm); err != nil {
			return l, err
		}
	} else {
		if wm, l, err = c.roundTrip(wm); err != nil {
			return l, err
		}

		c.interceptor(incomingCmds, wm)

		err = c.messenger.Write(c.ctx, l, wm, c.conn, c.address, c.id, 0, len(wm) > 1, c.conn.Close)
	}

	return l, err
}

func (c *connection) validateCommands(wm []*redis.Message) ([]string, error) {
	var transactionOpen bool
	incomingCmds := make([]string, len(wm))

	for i, m := range wm {
		var incomingCmd string
		if m.IsArray() {
			incomingCmd = strings.ToUpper(string(m.Array[0].Value))

			if t, ok := TransactionCommands[incomingCmd]; ok {
				switch t {
				case TransactionOpen:
					transactionOpen = true
				case TransactionClose:
					transactionOpen = false
				}
			}

			if _, ok := UnsupportedCommands[incomingCmd]; ok {
				return nil, fmt.Errorf("%v is unsupported", incomingCmd)
			}

			if incomingCmd == "CLUSTER" && len(m.Array) > 1 {
				// we only need to parse the next element if this is a CLUSTER command, for the
				// CLUSTER SLOTS and CLUSTER NODES cases
				incomingCmd += " " + strings.ToUpper(string(m.Array[1].Value))
			}

			incomingCmds[i] = incomingCmd
		}
	}

	if transactionOpen {
		return incomingCmds, fmt.Errorf("cannot leave an open transaction")
	}

	return incomingCmds, nil

}

func (c *connection) roundTrip(wm []*redis.Message) ([]*redis.Message, *zap.Logger, error) {
	l := c.log
	var err error

	var conn pool.ConnectionWrapper
	if conn, err = c.checkoutConnection(); err != nil {
		return nil, l, err
	}
	defer func() {
		l.Debug("Connection returned to pool (from roundTrip)")
		_ = conn.Return()
	}()

	l = c.log.With(zap.Uint64("upstream_id", conn.ID()))
	l.Debug("Connection checked out")

	if err = c.messenger.Write(c.ctx, l, wm, conn.Conn(), conn.Address().String(), conn.ID(), c.writeTimeout, false, conn.Close); err != nil {
		return nil, l, err
	}

	res, err := c.messenger.Read(c.ctx, l, conn.Conn(), conn.Address().String(), conn.ID(), c.readTimeout, len(wm), false, conn.Close)

	return res, l, err
}

func (c *connection) checkoutConnection() (conn pool.ConnectionWrapper, err error) {
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

func (c *connection) Write(wm []*redis.Message) error {
	err := c.messenger.Write(
		c.ctx, c.log, wm, c.conn, c.address, c.id,
		c.writeTimeout, len(wm) > 1, c.conn.Close,
	)

	return err
}

func (c *connection) Close() error {
	if c.Closed() {
		return nil
	}

	err := c.conn.Close()
	if err != nil {
		c.log.Error("Error closing connection", zap.Error(err))
		return err
	}

	c.Lock()
	defer c.Unlock()
	c.isClosed = true
	return nil
}

func (c *connection) Closed() bool {
	c.Lock()
	defer c.Unlock()

	return c.isClosed
}
