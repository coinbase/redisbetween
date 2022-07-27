package handlers

import (
	"context"
	"errors"
	"fmt"
	"github.com/coinbase/redisbetween/config"
	"github.com/coinbase/redisbetween/utils"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/coinbase/memcachedbetween/pool"
	"github.com/coinbase/redisbetween/redis"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

const ErrorMissingUpstream = "MISSING_UPSTREAM"

type connection struct {
	sync.Mutex

	log              *zap.Logger
	statsd           *statsd.Client
	ctx              context.Context
	readTimeout      time.Duration
	writeTimeout     time.Duration
	conn             net.Conn
	address          string
	upstream         string
	id               uint64
	kill             chan interface{}
	quit             chan interface{}
	interceptor      MessageInterceptor
	messenger        redis.Messenger
	reservations     *Reservations
	isClosed         bool
	redisLookup      config.Registry
	requestMirroring *config.RequestMirrorPolicy
}

type MessageInterceptor func(incomingCmds []string, m []*redis.Message)

func CommandConnection(log *zap.Logger, sd *statsd.Client, conn net.Conn, address, upstream string, id uint64, kill, quit chan interface{}, interceptor MessageInterceptor, reservations *Reservations, redisLookup config.Registry, listenerCfg *config.Listener) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:              log,
		statsd:           sd,
		ctx:              context.Background(),
		conn:             conn,
		address:          address,
		upstream:         upstream,
		id:               id,
		kill:             kill,
		quit:             quit,
		interceptor:      interceptor,
		messenger:        redis.WireMessenger{},
		reservations:     reservations,
		redisLookup:      redisLookup,
		requestMirroring: listenerCfg.Mirroring,
	}

	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxLogKey, log), utils.CtxStatsdKey, sd)
	c.processMessages(ctx)
}

func (c *connection) processMessages(ctx context.Context) {
	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	for {
		err := c.handleMessage(ctx)
		if err != nil {
			if err == io.EOF {
				c.isClosed = true
			} else {
				select {
				case <-c.kill:
					// ignore errors from force shutdown
				default:
					log.Error("Error handling message", zap.Error(err))
				}
			}
			return
		}
	}
}

func (c *connection) handleMessage(ctx context.Context) error {
	var err error
	var incomingCmds []string

	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	s := ctx.Value(utils.CtxStatsdKey).(*statsd.Client)

	defer func(start time.Time) {
		if len(incomingCmds) == 0 {
			incomingCmds = []string{"PARSERROR"}
		}

		_ = s.Timing("handle_message", time.Since(start), []string{
			fmt.Sprintf("success:%v", err == nil),
			fmt.Sprintf("command:%v", incomingCmds[0]),
		}, 1)
	}(time.Now())

	var wm []*redis.Message
	if wm, err = c.messenger.Read(c.ctx, log, c.conn, c.address, c.id, 0, 1, true, c.conn.Close); err != nil {
		return err
	}

	incomingCmds, err = c.validateCommands(wm)
	if err != nil {
		mm := []*redis.Message{redis.NewError([]byte(fmt.Sprintf("redisbetween: %v", err.Error())))}
		log.Debug("invalid commands", zap.Strings("commands", incomingCmds), zap.Error(err))
		err = c.messenger.Write(c.ctx, log, mm, c.conn, c.address, c.id, 0, false, c.conn.Close)
		return err
	}

	if isSubscriptionCommand(incomingCmds) {
		if err = handleSubscription(c, wm); err != nil {
			return err
		}

	} else if isBlockingCommand(incomingCmds) {
		if err = handleBlocker(c, wm); err != nil {
			return err
		}
	} else {
		r, ok := c.redisLookup.Lookup(c.upstream)

		if !ok {
			err := errors.New(ErrorMissingUpstream)
			log.Error("Cannot locate upstream for connection", zap.Error(err), zap.String("upstream", c.requestMirroring.Upstream))
			return err
		}

		client := r.(redis.ClientInterface)

		var res []*redis.Message
		if res, err = client.Call(ctx, wm); err != nil {
			return err
		}
		c.interceptor(incomingCmds, res)
		err = c.messenger.Write(c.ctx, log, res, c.conn, c.address, c.id, 0, len(res) > 1, c.conn.Close)

		if c.requestMirroring != nil {
			if r, ok := c.redisLookup.Lookup(c.requestMirroring.Upstream); ok {
				client := r.(redis.ClientInterface)
				if _, e := client.Call(ctx, wm); e != nil {
					log.Error("Failed to call mirror client", zap.Error(e), zap.String("upstream", c.requestMirroring.Upstream))
				}
			} else {
				err := errors.New(ErrorMissingUpstream)
				log.Error("Cannot locate upstream for mirroring", zap.Error(err), zap.String("upstream", c.requestMirroring.Upstream))
			}
		}
	}

	return err
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

func (c *connection) checkoutConnection() (pool.ConnectionWrapper, error) {
	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxLogKey, c.log), utils.CtxStatsdKey, c.statsd)
	if client, ok := c.redisLookup.Lookup(c.upstream); ok {
		return client.(redis.ClientInterface).CheckoutConnection(ctx)
	}

	return nil, errors.New(ErrorMissingUpstream)
}
