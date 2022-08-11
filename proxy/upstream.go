package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/redisbetween/config"
	"github.com/coinbase/redisbetween/redis"
)

type UpstreamManager interface {
	Add(c config.Upstream) error
	ConfigByName(name string) (*config.Upstream, bool)
	LookupByName(name string) (redis.ClientInterface, bool)
	Shutdown(ctx context.Context) error
}

func NewUpstreamManager(log *zap.Logger, sd *statsd.Client) UpstreamManager {
	return &upstreamManager{
		configsByName:   make(map[string]*config.Upstream),
		uniqueUpstreams: make(map[string]redis.ClientInterface),
		upstreamsByName: make(map[string]redis.ClientInterface),
		log:             log,
		sd:              sd,
	}
}

type upstreamManager struct {
	configsByName   map[string]*config.Upstream
	uniqueUpstreams map[string]redis.ClientInterface
	upstreamsByName map[string]redis.ClientInterface

	log *zap.Logger
	sd  *statsd.Client
	sync.RWMutex
}

func (m *upstreamManager) Add(cfg config.Upstream) error {
	m.Lock()
	defer m.Unlock()

	l := m.log.With(zap.String("label", cfg.Name), zap.String("address", cfg.Address))

	if ob, ok := m.upstreamsByName[cfg.Name]; ok {
		l.Debug("Upstream already initialized")
		m.uniqueUpstreams[cfg.Address] = ob
		return errors.New("ALREADY_PRESENT")
	}

	key := generateUniqueIdentifier(&cfg)
	if ob, ok := m.uniqueUpstreams[key]; ok {
		l.Debug("Upstream already initialized")
		m.upstreamsByName[cfg.Name] = ob
		return errors.New("ALREADY_PRESENT")
	}

	client, err := redis.NewClient(&redis.Options{
		Addr:         cfg.Address,
		Database:     cfg.Database,
		MinPoolSize:  cfg.MinPoolSize,
		MaxPoolSize:  cfg.MaxPoolSize,
		Readonly:     cfg.Readonly,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}, m.log, m.sd)

	if err != nil {
		return err
	}

	m.configsByName[cfg.Name] = &cfg
	m.uniqueUpstreams[key] = client
	m.upstreamsByName[cfg.Name] = client

	return nil
}

func generateUniqueIdentifier(u *config.Upstream) string {
	return fmt.Sprintf("%s:%v:%v", u.Address, u.Database, u.Readonly)
}

func (m *upstreamManager) ConfigByName(
	name string) (*config.Upstream, bool) {
	m.RLock()
	defer m.RUnlock()
	if c, ok := m.configsByName[name]; ok {
		return c, true
	}

	return nil, false
}

func (m *upstreamManager) LookupByName(name string) (redis.ClientInterface, bool) {
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
