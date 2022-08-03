package config

import (
	"context"
	"fmt"
	"github.com/coinbase/redisbetween/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io/fs"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func createConfig(t *testing.T, data string) *os.File {
	f, err := ioutil.TempFile("", "*-config.json")
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	bytes := []byte(data)
	assert.NoError(t, ioutil.WriteFile(f.Name(), bytes, fs.ModePerm))
	return f
}

func TestLoadConfigFromAFile(t *testing.T) {
	url := uuid.New().String()
	f := createConfig(t, fmt.Sprintf("{\"statsd\": \"%s\" }", url))

	defer func() {
		assert.NoError(t, os.Remove(f.Name()))
	}()

	opts := Options{ConfigUrl: f.Name(), PollInterval: 1000}
	config, err := Load(context.WithValue(context.TODO(), utils.CtxLogKey, zap.L()), &opts)
	assert.NoError(t, err)
	defer config.Stop()

	cfg, _ := config.Config()
	assert.Equal(t, url, cfg.Statsd)
}

func TestPollConfigFromAFile(t *testing.T) {
	url := uuid.New().String()
	f := createConfig(t, fmt.Sprintf("{\"statsd\": \"%s\" }", url))

	defer func() {
		assert.NoError(t, os.Remove(f.Name()))
	}()

	opts := Options{ConfigUrl: f.Name(), PollInterval: 1}
	config, err := Load(context.WithValue(context.TODO(), utils.CtxLogKey, zap.L()), &opts)
	assert.NoError(t, err)
	defer config.Stop()

	cfg, _ := config.Config()
	assert.Equal(t, url, cfg.Statsd)

	newUrl := uuid.New().String()
	jsonConfig := []byte(fmt.Sprintf("{\"statsd\": \"%s\" }", newUrl))
	assert.NoError(t, ioutil.WriteFile(f.Name(), jsonConfig, fs.ModePerm))

	<-config.OnUpdate()
	cfg, _ = config.Config()
	assert.NotEqual(t, url, cfg.Statsd)
	assert.Equal(t, newUrl, cfg.Statsd)
}

func TestLoadConfigToReadUpstreamAndListeners(t *testing.T) {
	upstream := uuid.New().String()
	f := createConfig(t, fmt.Sprintf(config, upstream))

	defer func() {
		assert.NoError(t, os.Remove(f.Name()))
	}()

	opts := Options{ConfigUrl: f.Name(), PollInterval: 1000}
	config, err := Load(context.WithValue(context.TODO(), utils.CtxLogKey, zap.L()), &opts)
	assert.NoError(t, err)
	defer config.Stop()

	cfg, _ := config.Config()
	assert.Equal(t, 1, len(cfg.Upstreams))
	assert.Equal(t, upstream, cfg.Upstreams[0].Name)
	assert.Equal(t, "localhost:7000", cfg.Upstreams[0].Address)
	assert.Equal(t, 1, cfg.Upstreams[0].MinPoolSize)
	assert.Equal(t, 1, cfg.Upstreams[0].MaxPoolSize)
	assert.Equal(t, 5*time.Second, cfg.Upstreams[0].ReadTimeout)
	assert.Equal(t, 5*time.Second, cfg.Upstreams[0].WriteTimeout)
	assert.False(t, cfg.Upstreams[0].Readonly)

	assert.Equal(t, 1, len(cfg.Listeners))
	assert.Equal(t, "test-listener", cfg.Listeners[0].Name)
	assert.Equal(t, "unix", cfg.Listeners[0].Network)
	assert.Equal(t, "redis-", cfg.Listeners[0].LocalSocketPrefix)
	assert.Equal(t, "-in", cfg.Listeners[0].LocalSocketSuffix)
	assert.Equal(t, upstream, cfg.Listeners[0].Target)
	assert.Equal(t, 1, cfg.Listeners[0].MaxSubscriptions)
	assert.Equal(t, 1, cfg.Listeners[0].MaxBlockers)
	assert.True(t, cfg.Listeners[0].Unlink)
}

const config = `{
  "url": "temp",
  "upstreams": [
    {
      "name": "%[1]v",
      "address": "localhost:7000",
      "database": 2,
      "maxPoolSize": 1,
      "minPoolSize": 1,
      "readonly": false
    }
  ],
  "listeners": [
    {
      "name": "test-listener",
      "network": "unix",
      "localSocketPrefix": "redis-",
      "localSocketSuffix": "-in",
      "target": "%[1]v",
      "maxSubscriptions": 1,
      "maxBlockers": 1,
      "unlink": true
    }
  ]
}`
