package proxy

import (
	"context"
	"fmt"
	red "github.cbhq.net/engineering/redis-proxy/redis"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/go-redis/redis/v8"
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

// ---
// Hello, this is Jordan at 11pm on thanksgiving. it looks like this redis client lib – while nice – is not as easy
// to work with for the purpose of building a proxy as https://github.com/mediocregopher/radix may be. it looks like
// radix supports a simpler interface for marshaling/unmarshaling redis messages, which means i can write my own types
// to encode/decode raw messages and pass them on to the downstream client conn while potentially not even parsing the
// underlying message into fully fledged types since i dont need them.

// next mission: replace go-redis with radix and see if that makes life easier. do this on a separate branch.

// another possibility is to only use the connection pooling / cluster protocol stuff from go-redis... ?
// ---

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
		return
	}

	ctx := context.TODO()
	e := ArgEncoder{}
	e.Encode(m)
	if e.Err != nil {
		return
	}
	//c.log.Debug("Request", zap.Strings("args", args))
	//fmt.Println("args", e.Args, "err", e.Err, "len(args)", len(e.Args))

	cmd := redis.NewCmd(ctx, e.Args...)
	err = c.client.Client.Process(ctx, cmd)
	if err != nil {
		return
	}

	c.log.Debug("Response", zap.String("result", cmd.String()))
	res, err := cmd.Result()
	if err != nil {
		return err
	}
	fmt.Println("cmd.Result()", res)

	// TODO have to write a CmdEncoder now
	// it should take val (cmd.Result()) and encode it using the RESP spec

	var out []byte
	switch b := res.(type) {
	case string:
		out = []byte(b)
	case int64:
		out = []byte(itoa(b))
	case []byte:
		out = b
	}
	if _, err = c.conn.Write(out); err != nil {
		return err
	}
	//
	//c.log.Debug("Response", zap.Int32("op_code", int32(resOpCode)), zap.Int("response_size", len(res.Wm)))
	return
}
