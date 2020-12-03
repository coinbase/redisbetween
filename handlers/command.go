package handlers

import (
	"context"
	"fmt"
	"github.cbhq.net/engineering/redis-proxy/config"
	"github.cbhq.net/engineering/redis-proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

type connection struct {
	log    *zap.Logger
	statsd *statsd.Client
	cfg    *config.Config

	ctx     context.Context
	conn    net.Conn
	address string
	id      uint64
	server  *redis.Server
	kill    chan interface{}
	events  chan interface{}
}

func CommandConnection(log *zap.Logger, sd *statsd.Client, cfg *config.Config, conn net.Conn, address string, id uint64, server *redis.Server, kill chan interface{}, events chan interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:     log,
		statsd:  sd,
		cfg:     cfg,
		ctx:     context.Background(),
		conn:    conn,
		address: address,
		id:      id,
		server:  server,
		kill:    kill,
		events:  events,
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

	// todo are you a CLUSTER SLOTS message? we need to rewrite you

	if wm, l, err = c.roundTrip(wm); err != nil {
		return l, err
	}

	// todo are you a MOVED message? we need to rewrite you

	err = WriteWireMessage(c.ctx, l, wm, c.conn, c.address, c.id, 0, c.conn.Close)
	return l, err
}

func (c *connection) roundTrip(wm *redis.Message) (*redis.Message, *zap.Logger, error) {
	l := c.log
	var err error

	var conn *redis.Connection
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

func (c *connection) checkoutConnection() (conn *redis.Connection, err error) {
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
		return redis.ConnectionError{Address: address, ID: id, Wrapped: ctx.Err(), Message: "failed to write"}
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
		return redis.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "failed to set write deadline"}
	}

	err = redis.Encode(nc, wm)

	if err != nil {
		_ = close()
		return redis.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "unable to write wire message to network"}
	}
	log.Debug("Write", zap.String("address", address))
	return nil
}

func ReadWireMessage(ctx context.Context, log *zap.Logger, nc net.Conn, address string, id uint64, readTimeout time.Duration, close func() error) (*redis.Message, error) {
	select {
	case <-ctx.Done():
		// We closeConnection the connection because we don't know if there is an unread message on the wire.
		_ = close()
		return nil, redis.ConnectionError{Address: address, ID: id, Wrapped: ctx.Err(), Message: "failed to read"}
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
		return nil, redis.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "failed to set read deadline"}
	}

	return redis.Decode(nc)

	//// We use an array here because it only costs 24 bytes on the stack and means we'll only need to
	//// reslice dst once instead of twice.
	//var headerBuf [24]byte
	//
	//// We do a ReadFull into an array here instead of doing an opportunistic ReadAtLeast into dst
	//// because there might be more than one wire message waiting to be read, for example when
	//// reading messages from an exhaust cursor.
	//_, err := io.ReadFull(nc, headerBuf[:])
	//if err != nil {
	//	// We closeConnection the connection because we don't know if there are other bytes left to read.
	//	_ = close()
	//	if err == io.EOF {
	//		return nil, err
	//	}
	//	return nil, redis.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "incomplete read of message header"}
	//}
	//
	//// read the length as an int32
	//size := 24 + ((int32(headerBuf[11])) | (int32(headerBuf[10]) << 8) | (int32(headerBuf[9]) << 16) | (int32(headerBuf[8]) << 24))
	//
	//log.Debug("Read header", zap.String("address", address), zap.Int("length", 24), zap.Int32("size", size-24), zap.String("hex", hex.EncodeToString(headerBuf[:])))
	//
	//if int(size) > cap(dst) {
	//	// Since we can't grow this slice without allocating, just allocate an entirely new slice.
	//	dst = make([]byte, 0, size)
	//}
	//// We need to ensure we don't accidentally read into a subsequent wire message, so we set the
	//// size to read exactly this wire message.
	//dst = dst[:size]
	//copy(dst, headerBuf[:])
	//
	//if size > 24 {
	//	_, err = io.ReadFull(nc, dst[24:])
	//	if err != nil {
	//		// We closeConnection the connection because we don't know if there are other bytes left to read.
	//		_ = close()
	//		return nil, redis.ConnectionError{Address: address, ID: id, Wrapped: err, Message: "incomplete read of full message"}
	//	}
	//
	//	max := int32(64)
	//	if size < max {
	//		max = size
	//	}
	//	log.Debug("Read", zap.String("address", address), zap.Int32("length", size-24), zap.String("hex", hex.EncodeToString(dst[24:max])))
	//}
	//
	//return dst, nil
}
