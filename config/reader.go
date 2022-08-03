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

type dynamicConfig struct {
	current        *Config
	version        Version
	stop           chan<- bool
	wg             *sync.WaitGroup
	updateChannels []chan<- bool
	sync.RWMutex
}

func Load(ctx context.Context, opts *Options) (DynamicConfig, error) {
	return newDynamicConfig(ctx, opts)
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
		log := log.With(zap.String("process", "config_poller"))

		defer wg.Done()
		defer close(updateChannel)

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
	// Since the channel already has update
	for _, channel := range d.updateChannels {
		select {
		case channel <- true:
		default:
		}
	}
}

func (d *dynamicConfig) OnUpdate() <-chan bool {
	d.Lock()
	defer d.Unlock()
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
	d.Lock()
	defer d.Unlock()
	defer d.wg.Wait()

	d.updateChannels = nil
	close(d.stop)
}
