package proxy

import (
	"fmt"
	"github.cbhq.net/engineering/redis-proxy/redis"
	"github.com/DataDog/datadog-go/statsd"
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
	defer func(start time.Time) {
		_ = c.statsd.Timing("handle_message", time.Since(start), []string{
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

	var m *redis.Message
	if m, err = redis.Decode(c.conn); err != nil {
		return
	}

	rcv, err := c.client.Do(m)
	if err != nil {
		errorMessage := fmt.Sprintf("-redis-proxy: %v\r\n", err.Error())
		_, err = c.conn.Write([]byte(errorMessage))
		if err != nil {
			return err
		}
		return
	}

	c.log.Debug("response", zap.String("result", string(rcv)))
	if _, err = c.conn.Write(rcv); err != nil {
		return err
	}
	return
}
