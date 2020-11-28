package proxy

import (
	"fmt"
	"github.cbhq.net/engineering/redis-proxy/redis"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"go.uber.org/zap"
	"io"
	"net"
	"runtime/debug"
	"time"
)

type connection struct {
	log    *zap.Logger
	statsd *statsd.Client
	conn   net.Conn
	client redis.Redis
	kill   chan interface{}
	buffer []byte
}

func handleConnection(log *zap.Logger, sd *statsd.Client, conn net.Conn, client redis.Redis, kill chan interface{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("Connection crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
		}
	}()

	c := connection{
		log:    log,
		statsd: sd,
		conn:   conn,
		client: client,
		kill:   kill,
	}
	c.processMessages()
}

func (c *connection) processMessages() {
	for {
		err := c.handleMessage()
		if err != nil {
			if err != io.EOF {
				select {
				case <-c.kill:
					// ignore errors from force shutdown
				default:
					c.log.Error("Error handling message", zap.Error(err))
				}
			}
			return
		}
	}
}

func (c *connection) handleMessage() (err error) {
	var command string

	defer func(start time.Time) {
		_ = c.statsd.Timing("handle_message", time.Since(start), []string{
			fmt.Sprintf("success:%v", err == nil),
			fmt.Sprintf("command:%v", command),
		}, 1)
	}(time.Now())

	var m *Message
	if m, err = Decode(c.conn); err != nil {
		return
	}

	// TODO return err for unsupported commands
	// TODO do array arguments work correctly?
	// TODO test all commands

	e := ArgEncoder{}
	e.Encode(m)
	if e.Err != nil {
		return
	}
	c.log.Debug("Request", zap.Strings("command", e.Args))

	rcv := resp2.RawMessage{}
	command = e.Args[0]
	err = c.client.Do(radix.Cmd(&rcv, command, e.Args[1:]...))
	if err != nil {
		return
	}
	c.log.Debug("Response", zap.String("result", string(rcv)))

	if _, err = c.conn.Write(rcv); err != nil {
		return err
	}
	return
}
