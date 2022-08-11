package proxy

import (
	"context"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/redisbetween/config"
	"github.com/coinbase/redisbetween/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"go.uber.org/zap"
)

func TestUpstreamManagerLocateByNameMissingUpstream(t *testing.T) {
	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)

	mgr := NewUpstreamManager(zap.NewNop(), sd)
	defer assert.NoError(t, mgr.Shutdown(context.Background()))

	res, ok := mgr.LookupByName(uuid.New().String())

	assert.False(t, ok)
	assert.Nil(t, res)
}

func TestUpstreamManagerToAddUpstream(t *testing.T) {
	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)

	mgr := NewUpstreamManager(zap.NewNop(), sd)
	defer assert.NoError(t, mgr.Shutdown(context.Background()))

	u := config.Upstream{Name: uuid.New().String(), Address: utils.RedisHost() + ":7006"}
	err = mgr.Add(u)
	assert.NoError(t, err)

	res, ok := mgr.LookupByName(u.Name)
	assert.True(t, ok)
	assert.Equal(t, u.Address, res.Address())
}

func TestUpstreamManagerToNotAddDuplicateUpstream(t *testing.T) {
	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)

	mgr := NewUpstreamManager(zap.NewNop(), sd)
	defer assert.NoError(t, mgr.Shutdown(context.Background()))

	u := config.Upstream{Name: uuid.New().String(), Address: utils.RedisHost() + ":7006"}
	err = mgr.Add(u)
	assert.NoError(t, err)
	err = mgr.Add(u)
	assert.Error(t, err)
}

func TestUpstreamManagerToShutdown(t *testing.T) {
	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)

	mgr := NewUpstreamManager(zap.NewNop(), sd)
	u := config.Upstream{Name: uuid.New().String(), Address: utils.RedisHost() + ":7006"}
	assert.NoError(t, mgr.Add(u))
	res, _ := mgr.LookupByName(u.Name)

	err = mgr.Shutdown(context.Background())

	assert.NoError(t, err)
	assert.Error(t, res.Close(context.Background()), "server is closed")
}
