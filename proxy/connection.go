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
	"strings"
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

	args, err := EncodeToArgs(m)
	c.log.Debug("request", zap.Strings("command", args))
	if len(args) == 0 || err != nil {
		return
	}
	command = strings.ToUpper(args[0])

	if !KnownCommand(command) {
		c.log.Debug("unknown command", zap.Strings("command", args))
	}

	// normal operation for non-clustered redis is to return an error for the CLUSTER command, which this
	// proxy will do according to this condition.
	if UnsupportedCommand(command) {
		c.log.Debug("unsupported command", zap.Strings("command", args))
		errorMessage := fmt.Sprintf("-redis-proxy: unsupported command %v\r\n", command)
		_, err = c.conn.Write([]byte(errorMessage))
		if err != nil {
			return err
		}
		return
	}

	rcv := resp2.RawMessage{}
	// when using RawMessage as the receiver, the error return value won't be populated
	_ = c.client.Do(radix.Cmd(&rcv, command, args[1:]...))
	c.log.Debug("response", zap.String("result", string(rcv)))

	if _, err = c.conn.Write(rcv); err != nil {
		return err
	}
	return
}
