package redis

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/memcachedbetween/pool"
	"github.com/coinbase/mongobetween/util"
	"go.uber.org/zap"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Options struct {
	addr         string
	database     int
	readTimeout  time.Duration
	writeTimeout time.Duration
	readonly     bool
	minPoolSize  int
	maxPoolSize  int
}

type Client struct {
	server    pool.ServerWrapper
	messenger Messenger
	config    *Options
}

// NewClient opens a connection pool to the redis server
// and returns a handle.
func NewClient(ctx context.Context, c *Options) (*Client, error) {
	server, err := connectToServer(ctx, c)

	if err != nil {
		return nil, err
	}

	return &Client{
		server:    server,
		messenger: WireMessenger{},
		config:    c,
	}, nil
}

// Call performs a round trip call to the redis upstream and
// returns the response message sent by the server. The method
// will fail if the command is blocking
func (r *Client) Call(ctx context.Context, msg []*Message) ([]*Message, error) {
	log, ok := ctx.Value("log").(*zap.Logger)

	if !ok {
		log = zap.L()
	}

	var err error
	var conn pool.ConnectionWrapper
	if conn, err = r.checkoutConnection(ctx); err != nil {
		return nil, err
	}

	if s, ok := ctx.Value("statsd").(*statsd.Client); ok {
		defer func(start time.Time) {
			_ = s.Timing("upstream_round_trip", time.Since(start), []string{
				fmt.Sprintf("upstream_id:%d", conn.ID()),
				fmt.Sprintf("address:%s", conn.Address().String()),
				fmt.Sprintf("success:%v", err == nil),
			}, 1)
		}(time.Now())
	}

	l := log.With(zap.Uint64("upstream_id", conn.ID()))
	defer func() {
		l.Debug("Connection returned to pool")
		_ = conn.Return()
	}()
	l.Debug("Connection checked out")

	if err = r.messenger.Write(ctx, l, msg, conn.Conn(), conn.Address().String(), conn.ID(), r.config.writeTimeout, false, conn.Close); err != nil {
		return nil, err
	}

	res, err := r.messenger.Read(ctx, l, conn.Conn(), conn.Address().String(), conn.ID(), r.config.readTimeout, len(msg), false, conn.Close)

	return res, err
}

// Close disconnects from the upstream server
func (r *Client) Close(ctx context.Context) error {
	return r.server.Disconnect(ctx)
}

func connectToServer(ctx context.Context, c *Options) (*pool.Server, error) {
	log, ok := ctx.Value("log").(*zap.Logger)
	if !ok {
		log = zap.L()
	}

	var opts []pool.ServerOption
	if s, ok := ctx.Value("statsd").(*statsd.Client); ok {
		opts = []pool.ServerOption{
			pool.WithConnectionPoolMonitor(func(*pool.Monitor) *pool.Monitor { return poolMonitor(s) }),
		}
	} else {
		opts = []pool.ServerOption{}
	}

	opts = append(opts,
		pool.WithMinConnections(func(uint64) uint64 { return uint64(c.minPoolSize) }),
		pool.WithMaxConnections(func(uint64) uint64 { return uint64(c.maxPoolSize) }),
	)

	var initCommand []byte
	if c.database > -1 {
		// if a db number has been specified, we need to issue a SELECT command before adding
		// that connection to the pool, so it's always pinned to the right db
		d := strconv.Itoa(c.database)
		initCommand = []byte("*2\r\n$6\r\nSELECT\r\n$" + strconv.Itoa(len(d)) + "\r\n" + d + "\r\n")
	} else if c.readonly {
		// if this pool is designated for replica reads, we need to set the READONLY flag on
		// the upstream connection before adding it to the pool. this is only supported by
		// clustered redis, so it cannot be combined with SELECT.
		initCommand = []byte("*1\r\n$8\r\nREADONLY\r\n")
	}

	if initCommand != nil {
		co := connectWithInitCommand(initCommand, log)
		opts = append(opts, pool.WithConnectionOptions(func(cos ...pool.ConnectionOption) []pool.ConnectionOption {
			return append(cos, co)
		}))
	}

	server, err := pool.ConnectServer(pool.Address(c.addr), opts...)
	return server, err
}

func (r *Client) checkoutConnection(ctx context.Context) (conn pool.ConnectionWrapper, err error) {
	if s, ok := ctx.Value("statsd").(*statsd.Client); ok {
		defer func(start time.Time) {
			addr := ""
			if conn != nil {
				addr = conn.Address().String()
			}
			_ = s.Timing("checkout_connection", time.Since(start), []string{
				fmt.Sprintf("address:%s", addr),
				fmt.Sprintf("success:%v", err == nil),
			}, 1)
		}(time.Now())
	}

	conn, err = r.server.Connection(ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func poolMonitor(sd *statsd.Client) *pool.Monitor {
	checkedOut, checkedIn := util.StatsdBackgroundGauge(sd, "pool.checked_out_connections", []string{})
	opened, closed := util.StatsdBackgroundGauge(sd, "pool.open_connections", []string{})

	return &pool.Monitor{
		Event: func(e *pool.Event) {
			snake := strings.ToLower(regexp.MustCompile("([a-z0-9])([A-Z])").ReplaceAllString(e.Type, "${1}_${2}"))
			name := fmt.Sprintf("pool_event.%s", snake)
			tags := []string{
				fmt.Sprintf("address:%s", e.Address),
				fmt.Sprintf("reason:%s", e.Reason),
			}
			switch e.Type {
			case pool.ConnectionCreated:
				opened(name, tags)
			case pool.ConnectionClosed:
				closed(name, tags)
			case pool.GetSucceeded:
				checkedOut(name, tags)
			case pool.ConnectionReturned:
				checkedIn(name, tags)
			default:
				_ = sd.Incr(name, tags, 1)
			}
		},
	}
}
func connectWithInitCommand(command []byte, logWith *zap.Logger) pool.ConnectionOption {
	co := pool.WithDialer(func(dialer pool.Dialer) pool.Dialer {
		return pool.DialerFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
			dlr := &net.Dialer{Timeout: 30 * time.Second}
			conn, err := dlr.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			_, err = conn.Write(command)
			if err != nil {
				logWith.Error("failed to write command", zap.Error(err))
				return nil, err
			}
			res := make([]byte, 5)
			_, err = io.ReadFull(conn, res)
			if err != nil || string(res) != "+OK\r\n" {
				logWith.Error("failed to read response", zap.Error(err), zap.String("response", string(res)))
				return nil, err
			}
			return conn, err
		})
	})
	return co
}
