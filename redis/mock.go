package redis

import (
	"context"
	"github.com/coinbase/memcachedbetween/pool"
)

type client struct {
	server pool.ServerWrapper
}

func (c client) Close(ctx context.Context) error {
	return nil
}

func (c client) Call(ctx context.Context, msg []*Message) ([]*Message, error) {
	panic("implement me")
}

func (c client) CheckoutConnection(ctx context.Context) (conn pool.ConnectionWrapper, err error) {
	return c.server.Connection(ctx)
}

func NewMockClient(server pool.ServerWrapper) ClientInterface {
	return &client{
		server: server,
	}
}
