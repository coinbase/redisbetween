package proxy

import (
	"bytes"
	"github.cbhq.net/engineering/redisbetween/redis"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"net"
	"time"
)

var defaultPingInterval = 5 * time.Second
var defaultReconnectDelay = 1 * time.Second

type Invalidator struct {
	conn          net.Conn
	clientID      int64
	upstream      string
	heartbeat     *time.Ticker
	stopHeartbeat chan struct{}
	opts          invalidatorOpts
	stopped       atomic.Bool
}

type ConnFunc func(network, addr string) (net.Conn, error)

func DefaultConnFunc(network, addr string) (net.Conn, error) {
	return net.Dial(network, addr)
}

type invalidatorOpts struct {
	connFunc ConnFunc
	log      *zap.Logger
}

type InvalidatorOpt func(*invalidatorOpts)

func InvalidatorConnFunc(connFunc ConnFunc) InvalidatorOpt {
	return func(opts *invalidatorOpts) {
		opts.connFunc = connFunc
	}
}
func InvalidatorLogger(logger *zap.Logger) InvalidatorOpt {
	return func(opts *invalidatorOpts) {
		opts.log = logger
	}
}

func NewInvalidator(upstream string, options ...InvalidatorOpt) (*Invalidator, error) {
	opts := invalidatorOpts{
		connFunc: DefaultConnFunc,
	}
	for _, opt := range options {
		opt(&opts)
	}
	if opts.log == nil {
		l, _ := zap.NewProduction()
		opts.log = l
	}
	opts.log = opts.log.With(zap.String("invalidator", upstream))

	i := &Invalidator{
		upstream:      upstream,
		stopHeartbeat: make(chan struct{}),
		opts:          opts,
	}
	err := i.connect()
	return i, err
}

func (i *Invalidator) Run(cache *Cache) {
	for {
		m, err := redis.Decode(i.conn)
		if err != nil {
			down := time.Now()
			var attempts int
			for err != nil {
				i.opts.log.Error(
					"reconnecting",
					zap.Error(err),
					zap.Duration("downtime", time.Since(down)),
					zap.Int("attempts", attempts),
				)
				if i.stopped.Load() {
					return
				}
				_ = i.close()
				i.conn = nil
				cache.Clear()
				err = i.connect()
				time.Sleep(defaultReconnectDelay)
				attempts++
			}
		}

		if m == nil || isPong(m) || !isInvalidation(m) {
			continue
		}
		for _, mm := range m.Array[2].Array {
			_ = cache.Del(mm.Value)
		}
	}
}

func (i *Invalidator) SubscribeCommand(prefixes []string) *redis.Message {
	parts := []string{"CLIENT", "TRACKING", "on", "REDIRECT", redis.Itoa(i.clientID), "BCAST"}
	for _, p := range prefixes {
		parts = append(parts, "PREFIX", p)
	}
	return redis.NewCommand(parts...)
}

func (i *Invalidator) Shutdown() error {
	i.stopped.Store(true)
	return i.close()
}

func (i *Invalidator) connect() error {
	var err error
	i.conn, err = i.opts.connFunc("tcp", i.upstream)
	if err != nil {
		return err
	}

	err = redis.Encode(i.conn, redis.NewCommand("CLIENT", "ID"))
	if err != nil {
		return err
	}
	m, err := redis.Decode(i.conn)
	if err != nil || !m.IsInt() {
		return err
	}
	id, _ := redis.Btoi64(m.Value)
	i.clientID = id
	i.heartbeat = time.NewTicker(defaultPingInterval)
	go i.keepalive()

	err = redis.Encode(i.conn, redis.NewCommand("SUBSCRIBE", "__redis__:invalidate"))
	if err != nil {
		return err
	}
	_, err = redis.Decode(i.conn)
	if err != nil {
		return err
	}
	return nil
}

func (i *Invalidator) keepalive() {
	for {
		select {
		case <-i.heartbeat.C:
			err := redis.Encode(i.conn, redis.NewCommand("PING", redis.Itoa(i.clientID)))
			if err != nil {
				i.opts.log.Error("keepalive error", zap.Error(err))
			}
		case <-i.stopHeartbeat:
			i.heartbeat.Stop()
			return
		}
	}
}

func (i *Invalidator) close() error {
	i.stopHeartbeat <- struct{}{}
	return i.conn.Close()
}

func isPong(m *redis.Message) bool {
	return m.IsArray() && bytes.Equal(m.Array[0].Value, []byte("pong"))
}

func isInvalidation(m *redis.Message) bool {
	return m.IsArray() && len(m.Array) >= 3 && m.Array[2].IsArray()
}
