package proxy

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/redisbetween/config"
	redis2 "github.com/coinbase/redisbetween/redis"
	"github.com/coinbase/redisbetween/utils"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func SetupProxy(t *testing.T, upstreamPort string, db int) func() {
	return SetupProxyAdvancedConfig(t, upstreamPort, db, 1, 1, false)
}

func SetupProxyAdvancedConfig(t *testing.T, upstreamPort string, db int, maxPoolSize int, id int, readonly bool) func() {
	t.Helper()

	uri := utils.RedisHost() + ":" + upstreamPort

	sd, err := statsd.New("localhost:8125")
	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), utils.CtxStatsdKey, sd)
	assert.NoError(t, err)

	cfg := &config.Config{
		Network:           "unix",
		LocalSocketPrefix: fmt.Sprintf("/var/tmp/redisbetween-%d-", id),
		LocalSocketSuffix: ".sock",
		Unlink:            true,
	}

	client, _ := redis2.NewClient(ctx, &redis2.Options{Addr: uri})
	lookup := func(string) redis2.ClientInterface {
		return client
	}
	proxy, err := NewProxy(ctx, cfg, "test", uri, db, 1, maxPoolSize, 1*time.Second, 1*time.Second, readonly, 1, 1, lookup)
	assert.NoError(t, err)
	go func() {
		err := proxy.Run()
		assert.NoError(t, err)
	}()

	time.Sleep(1 * time.Second) // todo find a more elegant way to do this

	return func() {
		proxy.Shutdown()
	}
}

func SetupStandaloneClient(t *testing.T, address string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Network: "unix", Addr: address, MaxRetries: 1})
	res := client.Do(context.Background(), "ping")
	if res.Err() != nil {
		_ = client.Close()
		// Use t.Fatalf instead of assert because we want to fail fast if the cluster is down.
		t.Fatalf("error pinging redis: %v", res.Err())
	}
	return client
}

func SetupClusterClient(t *testing.T, address string, readonly bool, id int) *redis.ClusterClient {
	t.Helper()
	opt := &redis.ClusterOptions{
		Addrs: []string{address},
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			// redis client patch that translates tcp connection attempts to the local socket instead
			if strings.Contains(network, "tcp") {
				host, port, err := net.SplitHostPort(addr)
				if err != nil {
					return nil, err
				}
				addr = fmt.Sprintf("/var/tmp/redisbetween-%d-%v-%v", id, host, port)
				if readonly {
					addr += "-ro"
				}
				addr += ".sock"
				network = "unix"
			}
			return net.Dial(network, addr)
		},
		MaxRetries: 1,
	}
	client := redis.NewClusterClient(opt)
	res := client.Do(context.Background(), "ping")
	if res.Err() != nil {
		_ = client.Close()
		// Use t.Fatalf instead of assert because we want to fail fast if the cluster is down.
		t.Fatalf("error pinging redis: %v", res.Err())
	}
	return client
}
