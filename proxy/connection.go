package proxy

import (
	"fmt"
	red "github.cbhq.net/engineering/redis-proxy/redis"
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
	client *red.Redis
	kill   chan interface{}
	buffer []byte
}

func handleConnection(log *zap.Logger, sd *statsd.Client, conn net.Conn, client *red.Redis, kill chan interface{}) {
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
	//var reqOpCode, resOpCode wiremessage.OpCode

	defer func(start time.Time) {
		_ = c.statsd.Timing("handle_message", time.Since(start), []string{
			fmt.Sprintf("success:%v", err == nil),
			//fmt.Sprintf("request_op_code:%v", reqOpCode),
			//fmt.Sprintf("response_op_code:%v", resOpCode),
		}, 1)
	}(time.Now())

	var m *Message
	if m, err = Decode(c.conn); err != nil {
		return err
	}

	fmt.Println("message", m)

	// TODO implement roundTrip and forward the message

	//
	//var op mongo.Operation
	//if op, err = mongo.Decode(wm); err != nil {
	//	return
	//}
	//
	//c.log.Debug("Request", zap.Int32("op_code", int32(op.OpCode())), zap.Int("request_size", len(wm)))
	//
	//isMaster = op.IsIsMaster()
	//req := &mongo.Message{
	//	Wm: wm,
	//	Op: op,
	//}
	//reqOpCode = op.OpCode()
	//
	//var res *mongo.Message
	//if res, err = c.roundTrip(req, isMaster); err != nil {
	//	return
	//}
	//if req.Op.Unacknowledged() {
	//	c.log.Debug("Unacknowledged request", zap.Int32("op_code", int32(resOpCode)))
	//	return
	//}
	//
	//resOpCode = res.Op.OpCode()
	//
	//if _, err = c.conn.Write(res.Wm); err != nil {
	//	return
	//}
	//
	//c.log.Debug("Response", zap.Int32("op_code", int32(resOpCode)), zap.Int("response_size", len(res.Wm)))
	return
}

//func (c *connection) roundTrip(msg *mongo.Message, isMaster bool) (*mongo.Message, error) {
//	if isMaster {
//		requestID := msg.Op.RequestID()
//		c.log.Debug("Non-proxied ismaster response", zap.Int32("request_id", requestID))
//		return mongo.IsMasterResponse(requestID, c.client.Description().Kind)
//	}
//
//	c.log.Debug("Proxying request to upstream server", zap.Int("request_size", len(msg.Wm)))
//	return c.client.RoundTrip(msg)
//}
