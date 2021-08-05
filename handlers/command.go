package handlers

import (
	"bytes"
	"context"
	"fmt"
	"github.com/coinbase/memcachedbetween/pool"
	"github.com/coinbase/redisbetween/redis"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

type connection struct {
	log          *zap.Logger
	statsd       *statsd.Client
	ctx          context.Context
	readTimeout  time.Duration
	writeTimeout time.Duration
	conn         net.Conn
	address      string
	id           uint64
	server       *pool.Server
	kill         chan interface{}
	interceptor  MessageInterceptor
}
type MessageInterceptor func(incomingCmds []string, m []*redis.Message)

var PipelineSignalStartKey = []byte("🔜")
var PipelineSignalEndKey = []byte("🔚")

func CommandConnection(log *zap.Logger, sd *statsd.Client, conn net.Conn, address string, readTimeout, writeTimeout time.Duration, id uint64, server *pool.Server, kill chan interface{}, interceptor MessageInterceptor) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:         log,
		statsd:      sd,
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

	var wm []*redis.Message
	if wm, err = ReadWireMessages(c.ctx, l, c.conn, c.address, c.id, 0, 1, true, c.conn.Close); err != nil {
		return l, err
	}

	incomingCmds, err := c.validateCommands(wm)
	if err != nil {
		mm := []*redis.Message{redis.NewError([]byte(fmt.Sprintf("redisbetween: %v", err.Error())))}
		c.log.Debug("invalid commands", zap.Strings("commands", incomingCmds), zap.Error(err))
		err = WriteWireMessages(c.ctx, l, mm, c.conn, c.address, c.id, 0, false, c.conn.Close)
		return l, err
	}

	if wm, l, err = c.roundTrip(wm); err != nil {
		return l, err
	}

	c.interceptor(incomingCmds, wm)

	err = WriteWireMessages(c.ctx, l, wm, c.conn, c.address, c.id, 0, len(wm) > 1, c.conn.Close)
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

	var conn *pool.Connection
	if conn, err = c.checkoutConnection(); err != nil {
		return nil, l, err
	}
	defer func() {
		_ = conn.Return()
	}()

	l = c.log.With(zap.Uint64("upstream_id", conn.ID()))
	l.Debug("Connection checked out")

	if err = WriteWireMessages(c.ctx, l, wm, conn.Conn(), conn.Address().String(), conn.ID(), c.writeTimeout, false, conn.Close); err != nil {
		return nil, l, err
	}

	res, err := ReadWireMessages(c.ctx, l, conn.Conn(), conn.Address().String(), conn.ID(), c.readTimeout, len(wm), false, conn.Close)

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

func WriteWireMessages(ctx context.Context, log *zap.Logger, wm []*redis.Message, nc net.Conn, address string, id uint64, writeTimeout time.Duration, wrapPipeline bool, close func() error) error {
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

	if wrapPipeline { // make dummy messages to pad out the pipeline signal responses
		s := redis.NewBulkBytes(nil) // redis nil response ($-1\r\n)
		e := redis.NewBulkBytes(nil) // redis nil response ($-1\r\n)
		wm = append(append([]*redis.Message{s}, wm...), e)
	}

	for _, m := range wm {
		err = redis.Encode(nc, m)
		if err != nil {
			_ = close()
			return pool.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "unable to write wire message to network"}
		}
	}

	log.Debug("Write", zap.String("address", address))
	return nil
}

func ReadWireMessages(ctx context.Context, log *zap.Logger, nc net.Conn, address string, id uint64, readTimeout time.Duration, readMin int, checkPipelineSignals bool, close func() error) ([]*redis.Message, error) {
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

	d := redis.NewDecoder(nc)
	var pipelineOpen bool
	wm := make([]*redis.Message, 0)
	for i := 0; i < readMin || (pipelineOpen && checkPipelineSignals); i++ {
		m, err := d.Decode()
		if err != nil {
			return nil, err
		}
		if checkPipelineSignals && isSignalMessage(m, PipelineSignalStartKey) {
			pipelineOpen = true
			continue
		} else if checkPipelineSignals && isSignalMessage(m, PipelineSignalEndKey) {
			pipelineOpen = false
			continue
		}
		wm = appendMessage(wm, m)
	}
	return wm, nil
}

// any message of length 2 (GET, for example) that passes the signal as its only argument
func isSignalMessage(m *redis.Message, signal []byte) bool {
	return len(m.Array) == 2 && bytes.Equal(signal, m.Array[1].Value)
}

// slightly more optimized than `append` for building message slices
func appendMessage(slice []*redis.Message, data ...*redis.Message) []*redis.Message {
	m := len(slice)
	n := m + len(data)
	if n > cap(slice) { // if necessary, reallocate. allocate double what's needed
		newSlice := make([]*redis.Message, (n+1)*2)
		copy(newSlice, slice)
		slice = newSlice
	}
	slice = slice[0:n]
	copy(slice[m:n], data)
	return slice
}
