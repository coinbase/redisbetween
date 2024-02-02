package messenger

import (
	"bytes"
	"context"
	"net"
	"time"

	"github.com/coinbase/memcachedbetween/pool"
	"github.com/d2army/redisbetween/redis"
	"go.uber.org/zap"
)

type Messenger interface {
	Read(ctx context.Context, log *zap.Logger, nc net.Conn, address string, id uint64, readTimeout time.Duration, readMin int, checkPipelineSignals bool, close func() error) ([]*redis.Message, error)
	Write(ctx context.Context, log *zap.Logger, wm []*redis.Message, nc net.Conn, address string, id uint64, writeTimeout time.Duration, wrapPipeline bool, close func() error) error
}

type WireMessenger struct{}

var PipelineSignalStartKey = []byte("🔜")
var PipelineSignalEndKey = []byte("🔚")

func (m WireMessenger) Read(ctx context.Context, log *zap.Logger, nc net.Conn, address string, id uint64, readTimeout time.Duration, readMin int, checkPipelineSignals bool, close func() error) ([]*redis.Message, error) {
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

func (m WireMessenger) Write(ctx context.Context, log *zap.Logger, wm []*redis.Message, nc net.Conn, address string, id uint64, writeTimeout time.Duration, wrapPipeline bool, close func() error) error {
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
