package redis

import (
	"context"
	"net"
	"time"
)

// Dialer is used to make network connections.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// DialerFunc is a type implemented by functions that can be used as a Dialer.
type DialerFunc func(ctx context.Context, network, address string) (net.Conn, error)

// DialContext implements the Dialer interface.
func (df DialerFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return df(ctx, network, address)
}

type connectionConfig struct {
	connectTimeout        time.Duration
	dialer                Dialer
	cmdMonitor            *CommandMonitor
	errorHandlingCallback func(error)
}

func newConnectionConfig(opts ...ConnectionOption) (*connectionConfig, error) {
	cfg := &connectionConfig{
		connectTimeout: 30 * time.Second,
		dialer:         nil,
	}

	for _, opt := range opts {
		err := opt(cfg)
		if err != nil {
			return nil, err
		}
	}

	if cfg.dialer == nil {
		cfg.dialer = &net.Dialer{Timeout: cfg.connectTimeout}
	}

	return cfg, nil
}

// ConnectionOption is used to configure a connection.
type ConnectionOption func(*connectionConfig) error

func withErrorHandlingCallback(fn func(error)) ConnectionOption {
	return func(c *connectionConfig) error {
		c.errorHandlingCallback = fn
		return nil
	}
}

// WithConnectTimeout configures the maximum amount of time a dial will wait for a
// Connect to complete. The default is 30 seconds.
func WithConnectTimeout(fn func(time.Duration) time.Duration) ConnectionOption {
	return func(c *connectionConfig) error {
		c.connectTimeout = fn(c.connectTimeout)
		return nil
	}
}

// WithDialer configures the Dialer to use when making a new connection to MongoDB.
func WithDialer(fn func(Dialer) Dialer) ConnectionOption {
	return func(c *connectionConfig) error {
		c.dialer = fn(c.dialer)
		return nil
	}
}

// WithMonitor configures a event for command monitoring.
func WithMonitor(fn func(*CommandMonitor) *CommandMonitor) ConnectionOption {
	return func(c *connectionConfig) error {
		c.cmdMonitor = fn(c.cmdMonitor)
		return nil
	}
}
