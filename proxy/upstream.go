package proxy

import (
	"context"
	"errors"
	"fmt"
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
	Shutdown(ctx context.Context) error
}

func NewUpstreamManager() UpstreamManager {
	return &upstreamManager{
		configsByName:   make(map[string]*config.Upstream),
		uniqueUpstreams: make(map[string]redis.ClientInterface),
		upstreamsByName: make(map[string]redis.ClientInterface),
	}
}

type upstreamManager struct {
	configsByName   map[string]*config.Upstream
	uniqueUpstreams map[string]redis.ClientInterface
	upstreamsByName map[string]redis.ClientInterface
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
		m.uniqueUpstreams[u.Address] = ob
		return errors.New("ALREADY_PRESENT")
	}

	key := generateUniqueIdentifier(u)
	if ob, ok := m.uniqueUpstreams[key]; ok {
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
	m.uniqueUpstreams[key] = client
	m.upstreamsByName[u.Name] = client

	return nil
}

func generateUniqueIdentifier(u *config.Upstream) string {
	return fmt.Sprintf("%s:%v:%v", u.Address, u.Database, u.Readonly)
}

func (m *upstreamManager) ConfigByName(_ context.Context, name string) (*config.Upstream, bool) {
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

func (m *upstreamManager) Shutdown(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	for _, u := range m.uniqueUpstreams {
		_ = u.Close(ctx)
	}

	return nil
}
