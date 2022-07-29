package proxy

import (
	"context"
	"errors"
	"github.com/coinbase/redisbetween/config"
	"github.com/coinbase/redisbetween/redis"
	"github.com/coinbase/redisbetween/utils"
	"go.uber.org/zap"
	"sync"
)

type UpstreamManager interface {
	Add(ctx context.Context, c *config.Upstream) error
	LookupByName(ctx context.Context, name string) (redis.ClientInterface, bool)
	LookupByAddress(ctx context.Context, address string) (redis.ClientInterface, bool)
	Shutdown(ctx context.Context) error
}

func NewUpstreamManager() UpstreamManager {
	return &upstreamManager{
		upstreamsByName:    make(map[string]redis.ClientInterface),
		upstreamsByAddress: make(map[string]redis.ClientInterface),
	}
}

type upstreamManager struct {
	upstreamsByName    map[string]redis.ClientInterface
	upstreamsByAddress map[string]redis.ClientInterface
	sync.RWMutex
}

func (m *upstreamManager) Add(ctx context.Context, u *config.Upstream) error {
	m.Lock()
	defer m.Unlock()
	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	l := log.With(zap.String("label", u.Name), zap.String("address", u.Address))

	if ob, ok := m.upstreamsByName[u.Name]; ok {
		l.Debug("Upstream already initialized")
		m.upstreamsByAddress[u.Address] = ob
		return errors.New("ALREADY_PRESENT")
	}

	if ob, ok := m.upstreamsByAddress[u.Address]; ok {
		l.Debug("Upstream already initialized")
		m.upstreamsByName[u.Name] = ob
		return errors.New("ALREADY_PRESENT")
	}

	client, err := redis.NewClient(ctx, &redis.Options{
		Addr:         u.Address,
		Database:     u.Database,
		MinPoolSize:  u.MinPoolSize,
		MaxPoolSize:  u.MaxPoolSize,
		Readonly:     u.Readonly,
		ReadTimeout:  u.ReadTimeout,
		WriteTimeout: u.WriteTimeout,
	})

	if err != nil {
		return err
	}

	m.upstreamsByAddress[u.Address] = client
	m.upstreamsByName[u.Name] = client

	return nil
}

func (m *upstreamManager) LookupByName(_ context.Context, name string) (redis.ClientInterface, bool) {
	m.RLock()
	defer m.RUnlock()
	if client, ok := m.upstreamsByName[name]; ok {
		return client, true
	}

	return nil, false
}

func (m *upstreamManager) LookupByAddress(_ context.Context, address string) (redis.ClientInterface, bool) {
	m.RLock()
	defer m.RUnlock()
	if client, ok := m.upstreamsByAddress[address]; ok {
		return client, true
	}

	return nil, false
}

func (m *upstreamManager) Shutdown(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	for name, u := range m.upstreamsByName {
		_ = u.Close(ctx)

		delete(m.upstreamsByName, name)
		delete(m.upstreamsByAddress, u.Address())
	}

	return nil
}
