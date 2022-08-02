package config

import (
	"context"
	"encoding/json"
	"github.com/coinbase/redisbetween/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestCreateANewConfig(t *testing.T) {
	c := configAlias{}
	assert.NoError(t, json.Unmarshal([]byte(baseConfig), &c))

	cfg := New(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), &c)
	assert.Equal(t, zap.InfoLevel, cfg.Level)
	assert.Equal(t, "upstream2", cfg.Listeners[0].Mirroring.Upstream)
}

func TestConfigShouldSkipUpstreamMissingName(t *testing.T) {
	c := configAlias{}
	assert.NoError(t, json.Unmarshal([]byte(missingNameUpstreamConfig), &c))

	cfg := New(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), &c)
	assert.Equal(t, 0, len(cfg.Upstreams))
}

func TestConfigShouldSkipUpstreamWithDuplicateName(t *testing.T) {
	c := configAlias{}
	assert.NoError(t, json.Unmarshal([]byte(duplicateNameUpstreamConfig), &c))

	cfg := New(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), &c)
	assert.Equal(t, 1, len(cfg.Upstreams))
	assert.Equal(t, "localhost:7000", cfg.Upstreams[0].Address)
}

func TestConfigShouldForIncompleteUpstream(t *testing.T) {
	c := configAlias{}
	assert.NoError(t, json.Unmarshal([]byte(incompleteUpstreamConfig), &c))

	cfg := New(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), &c)
	assert.Equal(t, 1, len(cfg.Upstreams))
	assert.Equal(t, "upstream", cfg.Upstreams[0].Name)
	assert.Equal(t, "localhost:7000", cfg.Upstreams[0].Address)
	assert.Equal(t, 5*time.Second, cfg.Upstreams[0].ReadTimeout)
	assert.Equal(t, 5*time.Second, cfg.Upstreams[0].WriteTimeout)
	assert.Equal(t, 0, cfg.Upstreams[0].Database)
	assert.Equal(t, 1, cfg.Upstreams[0].MinPoolSize)
	assert.Equal(t, 10, cfg.Upstreams[0].MaxPoolSize)
	assert.False(t, cfg.Upstreams[0].Readonly)
}

func TestConfigShouldSkipListenerMissingTarget(t *testing.T) {
	c := configAlias{}
	assert.NoError(t, json.Unmarshal([]byte(missingTargetListener), &c))

	cfg := New(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), &c)
	assert.Equal(t, 0, len(cfg.Listeners))
}

func TestConfigShouldSkipListenerWithInvalidNetwork(t *testing.T) {
	c := configAlias{}
	assert.NoError(t, json.Unmarshal([]byte(invalidNetworkListenerConfig), &c))

	cfg := New(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), &c)
	assert.Equal(t, 0, len(cfg.Listeners))
}

func TestConfigShouldSkipMirroringConfigOnListener(t *testing.T) {
	c := configAlias{}
	assert.NoError(t, json.Unmarshal([]byte(incompleteMirroringListenerConfig), &c))

	cfg := New(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), &c)
	assert.Equal(t, 1, len(cfg.Listeners))
	assert.Nil(t, cfg.Listeners[0].Mirroring)
}

func TestConfigShouldAddDefaultValuesForMissingListenerFields(t *testing.T) {
	c := configAlias{}
	assert.NoError(t, json.Unmarshal([]byte(incompleteListenerConfig), &c))

	cfg := New(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), &c)
	assert.Equal(t, 1, len(cfg.Listeners))
	assert.Equal(t, "test-listener", cfg.Listeners[0].Name)
	assert.Equal(t, "unix", cfg.Listeners[0].Network)
	assert.Equal(t, "upstream", cfg.Listeners[0].Target)
	assert.Equal(t, zap.InfoLevel, cfg.Listeners[0].LogLevel)
	assert.Equal(t, "/var/tmp/redisbetween-", cfg.Listeners[0].LocalSocketPrefix)
	assert.Equal(t, ".sock", cfg.Listeners[0].LocalSocketSuffix)
	assert.Equal(t, 1, cfg.Listeners[0].MaxSubscriptions)
	assert.Equal(t, 1, cfg.Listeners[0].MaxBlockers)
	assert.Nil(t, cfg.Listeners[0].Mirroring)
}

const baseConfig = `
{
  "url": "temp",
  "upstreams": [
    {
      "name": "upstream1",
      "address": "localhost:7000",
      "database": 2,
      "maxPoolSize": 1,
      "minPoolSize": 1,
      "readTimeout": "30s",
      "writeTimeout": "15s",
      "readonly": false
    }
  ],
  "listeners": [
    {
      "name": "test-listener",
      "network": "unix",
      "localSocketPrefix": "redis-",
      "localSocketSuffix": "-in",
      "target": "upstream1",
      "maxSubscriptions": 1,
      "maxBlockers": 1,
      "unlink": true,
      "mirroring": {
        "upstream": "upstream2"
      }
    }
  ]
}
`

const missingNameUpstreamConfig = `
{
  "url": "temp",
  "upstreams": [
    {
      "name": "",
      "address": "localhost:7000",
      "database": 2,
      "maxPoolSize": 1,
      "minPoolSize": 1,
      "readTimeout": "30s",
      "writeTimeout": "15s",
      "readonly": false
    }
  ],
  "listeners": []
}
`

const duplicateNameUpstreamConfig = `
{
  "url": "temp",
  "upstreams": [
    {
      "name": "upstream",
      "address": "localhost:7000",
      "database": 2,
      "maxPoolSize": 1,
      "minPoolSize": 1,
      "readTimeout": "30s",
      "writeTimeout": "15s",
      "readonly": false
    },
    {
      "name": "upstream",
      "address": "localhost:7001",
      "database": 2,
      "maxPoolSize": 1,
      "minPoolSize": 1,
      "readTimeout": "30s",
      "writeTimeout": "15s",
      "readonly": false
    }
  ],
  "listeners": []
}
`

const incompleteUpstreamConfig = `
{
  "url": "temp",
  "upstreams": [
    {
      "name": "upstream",
      "address": "localhost:7000"
    }
  ],
  "listeners": []
}
`

const missingTargetListener = `
{
  "url": "temp",
  "upstreams": [],
  "listeners": [
    {
      "name": "test-listener",
      "network": "unix",
      "localSocketPrefix": "redis-",
      "localSocketSuffix": "-in",
      "maxSubscriptions": 1,
      "maxBlockers": 1,
      "unlink": true
    }
  ]
}
`

const invalidNetworkListenerConfig = `
{
  "url": "temp",
  "upstreams": [],
  "listeners": [
    {
      "name": "test-listener",
      "network": "udp",
      "target": "upstream",
      "localSocketPrefix": "redis-",
      "localSocketSuffix": "-in",
      "maxSubscriptions": 1,
      "maxBlockers": 1,
      "unlink": true
    }
  ]
}
`

const incompleteListenerConfig = `
{
  "url": "temp",
  "upstreams": [],
  "listeners": [
    {
      "name": "test-listener",
      "network": "unix",
      "target": "upstream"
    }
  ]
}
`
const incompleteMirroringListenerConfig = `
{
  "url": "temp",
  "upstreams": [],
  "listeners": [
    {
      "name": "test-listener",
      "network": "unix",
      "target": "upstream",
	  "mirroring": {}
    }
  ]
}
`
