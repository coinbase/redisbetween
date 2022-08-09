package config

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildFromOptionsShouldReturnStaticConfigFromOpts(t *testing.T) {
	opts := Options{
		Network:           "unix",
		LocalSocketPrefix: uuid.New().String(),
		LocalSocketSuffix: uuid.New().String(),
		Unlink:            false,
		Pretty:            false,
		Statsd:            uuid.New().String(),
		Level:             0,
		Upstreams: []upstream{upstream{
			UpstreamConfigHost: uuid.New().String(),
			Label:              uuid.New().String(),
			MaxPoolSize:        10,
			MinPoolSize:        0,
			Database:           0,
			ReadTimeout:        10,
			WriteTimeout:       10,
			Readonly:           false,
			MaxSubscriptions:   8,
			MaxBlockers:        9,
		}},
	}
	config, err := BuildFromOptions(context.Background(), &opts)
	assert.NoError(t, err)

	assert.Equal(t, opts.Statsd, config.Statsd)
	assert.Equal(t, 1, len(config.Upstreams))
	assert.Equal(t, opts.Upstreams[0].Label, config.Upstreams[0].Name)
	assert.Equal(t, opts.Upstreams[0].UpstreamConfigHost, config.Upstreams[0].Address)
	assert.Equal(t, opts.Upstreams[0].MinPoolSize, config.Upstreams[0].MinPoolSize)
	assert.Equal(t, opts.Upstreams[0].MaxPoolSize, config.Upstreams[0].MaxPoolSize)
	assert.Equal(t, opts.Upstreams[0].Database, config.Upstreams[0].Database)
	assert.Equal(t, opts.Upstreams[0].ReadTimeout, config.Upstreams[0].ReadTimeout)
	assert.Equal(t, opts.Upstreams[0].WriteTimeout, config.Upstreams[0].WriteTimeout)
	assert.Equal(t, opts.Upstreams[0].Readonly, config.Upstreams[0].Readonly)
	assert.Equal(t, 1, len(config.Listeners))
	assert.Equal(t, opts.LocalSocketPrefix, config.Listeners[0].LocalSocketPrefix)
	assert.Equal(t, opts.LocalSocketSuffix, config.Listeners[0].LocalSocketSuffix)
	assert.Equal(t, opts.Upstreams[0].Label, config.Listeners[0].Name)
	assert.Equal(t, opts.Upstreams[0].Label, config.Listeners[0].Target)
	assert.Equal(t, opts.Upstreams[0].MaxSubscriptions, config.Listeners[0].MaxSubscriptions)
	assert.Equal(t, opts.Upstreams[0].MaxBlockers, config.Listeners[0].MaxBlockers)
	assert.Equal(t, opts.Unlink, config.Listeners[0].Unlink)
	assert.Equal(t, opts.Network, config.Listeners[0].Network)
}
