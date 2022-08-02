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
	ConfigByName(ctx context.Context, name string) (*config.Upstream, bool)
	LookupByName(ctx context.Context, name string) (redis.ClientInterface, bool)
	LookupByAddress(ctx context.Context, address string) (redis.ClientInterface, bool)
	Shutdown(ctx context.Context) error
}

func NewUpstreamManager() UpstreamManager {
	return &upstreamManager{
		configsByName:      make(map[string]*config.Upstream),
		upstreamsByName:    make(map[string]redis.ClientInterface),
		upstreamsByAddress: make(map[string]redis.ClientInterface),
	}
}

type upstreamManager struct {
	configsByName      map[string]*config.Upstream
	upstreamsByName    map[string]redis.ClientInterface
	upstreamsByAddress map[string]redis.ClientInterface
	sync.RWMutex
}

func (m *upstreamManager) Add(ctx context.Context, u *config.Upstream) error {
	m.Lock()
	defer m.Unlock()

	// Make a local copy of the config
	cfg := &config.Upstream{}
	*cfg = *u

	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	l := log.With(zap.String("label", cfg.Name), zap.String("address", cfg.Address))

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
		Addr:         cfg.Address,
		Database:     cfg.Database,
		MinPoolSize:  cfg.MinPoolSize,
		MaxPoolSize:  cfg.MaxPoolSize,
		Readonly:     cfg.Readonly,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	if err != nil {
		return err
	}

	m.configsByName[u.Name] = cfg
	m.upstreamsByAddress[u.Address] = client
	m.upstreamsByName[u.Name] = client

	return nil
}

func (m *upstreamManager) ConfigByName(ctx context.Context, name string) (*config.Upstream, bool) {
	m.RLock()
	defer m.RUnlock()
	if c, ok := m.configsByName[name]; ok {
		return c, true
	}

	return nil, false
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
