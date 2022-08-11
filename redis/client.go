package redis

import (
	"context"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/memcachedbetween/pool"
	"github.com/coinbase/mongobetween/util"
)

type Options struct {
	Addr         string
	Database     int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Readonly     bool
	MinPoolSize  int
	MaxPoolSize  int
}

type Client struct {
	server    pool.ServerWrapper
	messenger Messenger
	config    Options

	sd  *statsd.Client
	log *zap.Logger
}

type ClientInterface interface {
	Address() string
	Call(ctx context.Context, msg []*Message) ([]*Message, error)
	CheckoutConnection(ctx context.Context) (conn pool.ConnectionWrapper, err error)
	Close(ctx context.Context) error
}

// NewClient opens a connection pool to the redis server
// and returns a handle.
func NewClient(c *Options, log *zap.Logger, sd *statsd.Client) (ClientInterface, error) {
	config := *c
	server, err := connectToServer(&config, log, sd)

	if err != nil {
		return nil, err
	}

	return &Client{
		server:    server,
		messenger: WireMessenger{},
		config:    config,
		sd:        sd,
		log:       log,
	}, nil
}

func (r *Client) Address() string {
	return r.config.Addr
}

// Call performs a round trip call to the redis upstream and
// returns the response message sent by the server. The method
// will fail if the command is blocking
func (r *Client) Call(ctx context.Context, msg []*Message) ([]*Message, error) {
	log := r.log

	var err error
	var conn pool.ConnectionWrapper
	if conn, err = r.CheckoutConnection(ctx); err != nil {
		return nil, err
	}
	defer func() {
		log.Debug("Connection returned to pool", zap.Uint64("upstream_id", conn.ID()))
		_ = conn.Return()
	}()

	defer func(start time.Time) {
		id, address := conn.ID(), conn.Address().String()
		_ = r.sd.Timing("upstream_round_trip", time.Since(start), []string{
			fmt.Sprintf("upstream_id:%d", id),
			fmt.Sprintf("address:%s", address),
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

	log = log.With(zap.Uint64("upstream_id", conn.ID()))
	log.Debug("Connection checked out")

	if err = r.messenger.Write(ctx, log, msg, conn.Conn(), conn.Address().String(), conn.ID(), r.config.WriteTimeout, false, conn.Close); err != nil {
		return nil, err
	}

	res, err := r.messenger.Read(ctx, log, conn.Conn(), conn.Address().String(), conn.ID(), r.config.ReadTimeout, len(msg), false, conn.Close)

	return res, err
}

// Close disconnects from the upstream server
func (r *Client) Close(ctx context.Context) error {
	return r.server.Disconnect(ctx)
}

func connectToServer(c *Options, log *zap.Logger, sd *statsd.Client) (*pool.Server, error) {
	sd, err := util.StatsdWithTags(sd, []string{fmt.Sprintf("upstream:%s", c.Addr)})

	if err != nil {
		return nil, err
	}

	opts := []pool.ServerOption{
		pool.WithConnectionPoolMonitor(func(*pool.Monitor) *pool.Monitor { return poolMonitor(sd) }),
		pool.WithMinConnections(func(uint64) uint64 { return uint64(c.MinPoolSize) }),
		pool.WithMaxConnections(func(uint64) uint64 { return uint64(c.MaxPoolSize) }),
	}

	var initCommand []byte
	if c.Database > -1 {
		// if a db number has been specified, we need to issue a SELECT command before adding
		// that connection to the pool, so it's always pinned to the right db
		d := strconv.Itoa(c.Database)
		initCommand = []byte("*2\r\n$6\r\nSELECT\r\n$" + strconv.Itoa(len(d)) + "\r\n" + d + "\r\n")
	} else if c.Readonly {
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

	server, err := pool.ConnectServer(pool.Address(c.Addr), opts...)
	return server, err
}

// CheckoutConnection returns a connection from the pool. The method is temporarily made public
func (r *Client) CheckoutConnection(ctx context.Context) (conn pool.ConnectionWrapper, err error) {
	defer func(start time.Time) {
		addr := ""
		if conn != nil {
			addr = conn.Address().String()
		}
		_ = r.sd.Timing("checkout_connection", time.Since(start), []string{
			fmt.Sprintf("address:%s", addr),
			fmt.Sprintf("success:%v", err == nil),
		}, 1)
	}(time.Now())

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
func connectWithInitCommand(command []byte, log *zap.Logger) pool.ConnectionOption {
	co := pool.WithDialer(func(dialer pool.Dialer) pool.Dialer {
		return pool.DialerFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
			dlr := &net.Dialer{Timeout: 30 * time.Second}
			conn, err := dlr.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			_, err = conn.Write(command)
			if err != nil {
				log.Error("failed to write command", zap.Error(err))
				return nil, err
			}
			res := make([]byte, 5)
			_, err = io.ReadFull(conn, res)
			if err != nil || string(res) != "+OK\r\n" {
				log.Error("failed to read response", zap.Error(err), zap.String("response", string(res)))
				return nil, err
			}
			return conn, err
		})
	})
	return co
}
