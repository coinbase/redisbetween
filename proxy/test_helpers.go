package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/d2army/redisbetween/config"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func RedisHost() string {
	h := os.Getenv("REDIS_HOST")
	if h != "" {
		return h
	}
	return "127.0.0.1"
}

func SetupProxy(t *testing.T, upstreamPort string, db int) func() {
	return SetupProxyAdvancedConfig(t, upstreamPort, db, 1, 1, false)
}

func SetupProxyAdvancedConfig(t *testing.T, upstreamPort string, db int, maxPoolSize int, id int, readonly bool) func() {
	t.Helper()

	uri := RedisHost() + ":" + upstreamPort

	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)

	cfg := &config.Config{
		Network:           "unix",
		LocalSocketPrefix: fmt.Sprintf("/var/tmp/redisbetween-%d-", id),
		LocalSocketSuffix: ".sock",
		Unlink:            true,
	}
	up := config.Upstream{
		UpstreamConfigHost: uri,
		Label:              "test",
		Database:           db,
		MinPoolSize:        1,
		MaxPoolSize:        maxPoolSize,
		Readonly:           readonly,
		ReadTimeout:        1 * time.Second,
		WriteTimeout:       1 * time.Second,
		MaxSubscriptions:   1,
		MaxBlockers:        1,
	}
	cfg.Upstreams = []config.Upstream{up}

	proxy, err := NewProxy(zap.L(), sd, cfg, 0)
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
