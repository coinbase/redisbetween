package config

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"github.com/coinbase/redisbetween/utils"
	"github.com/hashicorp/go-getter"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

type Version int64

type DynamicConfig interface {
	Config() (*Config, Version)
	OnUpdate() <-chan bool
	Stop()
}

func Load(ctx context.Context, opts *Options) (DynamicConfig, error) {
	if opts.ConfigUrl == "" {
		return newStaticConfig(ctx, opts)
	}
	return newDynamicConfig(ctx, opts)
}

func newStaticConfig(_ context.Context, opts *Options) (*staticConfig, error) {
	var listeners []*Listener
	var upstreams []*Upstream

	for _, u := range opts.Upstreams {
		upstreams = append(upstreams, &Upstream{
			Name:         u.Label,
			Address:      u.UpstreamConfigHost,
			Database:     u.Database,
			MaxPoolSize:  u.MaxPoolSize,
			MinPoolSize:  u.MinPoolSize,
			ReadTimeout:  u.ReadTimeout,
			WriteTimeout: u.WriteTimeout,
			Readonly:     u.Readonly,
		})

		listeners = append(listeners, &Listener{
			Name:              u.Label,
			Network:           opts.Network,
			LocalSocketPrefix: opts.LocalSocketPrefix,
			LocalSocketSuffix: opts.LocalSocketSuffix,
			LogLevel:          opts.Level,
			Target:            u.Label,
			MaxSubscriptions:  u.MaxSubscriptions,
			MaxBlockers:       u.MaxBlockers,
			Unlink:            opts.Unlink,
			Mirroring:         nil,
		})
	}

	currentConfig := &Config{
		Pretty:    opts.Pretty,
		Statsd:    opts.Statsd,
		Level:     opts.Level,
		Listeners: listeners,
		Upstreams: upstreams,
	}

	return &staticConfig{
		current:       currentConfig,
		version:       Version(time.Now().UnixMilli()),
		updateChannel: make(chan bool),
	}, nil
}

func newDynamicConfig(ctx context.Context, opts *Options) (*dynamicConfig, error) {
	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	stop := make(chan bool)
	configUpdate := make(chan *Config)

	var wg sync.WaitGroup
	current, currentHash, err := readConfig(opts, log)

	if err != nil {
		return nil, err
	}

	dyn := &dynamicConfig{
		current: current,
		version: Version(time.Now().UnixMilli()),
		stop:    stop,
		wg:      &wg,
	}

	wg.Add(2)
	go func(update <-chan *Config, stop <-chan bool) {
		defer wg.Done()
		for {
			select {
			case c := <-update:
				dyn.updateConfig(c)
			case <-stop:
				return
			}
		}
	}(configUpdate, stop)

	go func(opts *Options, updateChannel chan<- *Config, stopChannel <-chan bool) {
		defer wg.Done()

		log := log.With(zap.String("process", "config_poller"))

		interval := opts.PollInterval
		lastHash := currentHash

		for {
			select {
			case <-time.After(interval):
				c, currentHash, err := readConfig(opts, log)
				if err != nil {
					log.Error("Failed to fetch config", zap.Error(err))
					continue
				}

				if lastHash != currentHash {
					updateChannel <- c
					lastHash = currentHash
				}
			case <-stopChannel:
				return
			}
		}

	}(opts, configUpdate, stop)

	return dyn, nil
}

type staticConfig struct {
	current       *Config
	version       Version
	updateChannel chan bool
}

func (s *staticConfig) Config() (*Config, Version) {
	return s.current, s.version
}

func (s *staticConfig) OnUpdate() <-chan bool {
	return s.updateChannel
}

func (s *staticConfig) Stop() {
	close(s.updateChannel)
}

type dynamicConfig struct {
	current        *Config
	version        Version
	stop           chan<- bool
	wg             *sync.WaitGroup
	updateChannels []chan<- bool
	sync.RWMutex
}

func readConfig(opts *Options, log *zap.Logger) (*Config, string, error) {
	f, err := ioutil.TempFile("", "*.json")
	if err != nil {
		return nil, "", err
	}

	err = f.Close()
	if err != nil {
		return nil, "", err
	}

	pwd, _ := os.Getwd()
	client := &getter.Client{
		Ctx:  context.Background(),
		Src:  opts.ConfigUrl,
		Dst:  f.Name(),
		Pwd:  pwd,
		Mode: getter.ClientModeFile,
	}

	if err := client.Get(); err != nil {
		return nil, "", err
	}

	f, err = os.Open(f.Name())
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	if err != nil {
		return nil, "", err
	}

	body, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, "", err
	}

	hash := md5.Sum(body)

	alias := configAlias{
		Pretty: opts.Pretty,
		Statsd: opts.Statsd,
		Level:  opts.Level.String(),
	}

	if err = json.Unmarshal(body, &alias); err != nil {
		return nil, "", err
	}

	ctx := context.WithValue(context.TODO(), utils.CtxLogKey, log)
	cfg := New(ctx, &alias)

	return cfg, hex.EncodeToString(hash[:]), nil
}

func (d *dynamicConfig) updateConfig(c *Config) {
	d.Lock()
	defer d.Unlock()
	d.current = c
	d.version = Version(time.Now().UnixMilli())

	// Prevent blocking in case the channel is not consumed.
	// Since the channel already has an update
	for _, channel := range d.updateChannels {
		select {
		case channel <- true:
		default:
		}
	}
}

func (d *dynamicConfig) OnUpdate() <-chan bool {
	channel := make(chan bool, 1)

	d.updateChannels = append(d.updateChannels, channel)
	return channel
}

func (d *dynamicConfig) Config() (*Config, Version) {
	d.RLock()
	defer d.RUnlock()
	return d.current, d.version
}

func (d *dynamicConfig) Stop() {
	defer d.wg.Wait()

	d.updateChannels = nil
	close(d.stop)
}
