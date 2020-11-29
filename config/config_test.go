package config

import (
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"os"
	"testing"
)

func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
}

func TestParseFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redis-proxy",
		"-statsd", "statsd:1234",
		"-loglevel", "debug",
		"-network", "unix",
		"-unlink",
		"/tmp/redis1.sock=redis://localhost:27127/0?poolsize=5&label=cluster1",
		"/tmp/redis2.sock=redis://localhost:27128/1?poolsize=10&label=cluster2&cluster=true",
	}

	resetFlags()
	c, err := parseFlags()
	fmt.Println(err)
	assert.Nil(t, err)

	assert.Equal(t, "statsd:1234", c.statsd)
	assert.Equal(t, zapcore.DebugLevel, c.LogLevel())
	assert.Equal(t, "unix", c.network)
	assert.True(t, c.unlink)

	assert.Equal(t, 2, len(c.clients))
	client1 := c.clients[0]
	client2 := c.clients[1]
	if client1.label == "cluster2" {
		temp := client1
		client1 = client2
		client2 = temp
	}

	assert.Equal(t, "cluster1", client1.label)
	assert.Equal(t, "redis://localhost:27127/0?poolsize=5&label=cluster1", client1.host)
	assert.Equal(t, 5, client1.poolSize)

	assert.Equal(t, "cluster2", client2.label)
	assert.Equal(t, "redis://localhost:27128/1?poolsize=10&label=cluster2&cluster=true", client2.host)
	assert.Equal(t, 10, client2.poolSize)
	assert.True(t, client2.cluster)
}

func TestEnvExpansion(t *testing.T) {
	env := "TEST_ENV"
	oldEnv := os.Getenv(env)
	defer func() { _ = os.Setenv(env, oldEnv) }()
	_ = os.Setenv(env, "env_value")

	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redis-proxy",
		"-statsd", "before_${TEST_ENV}_after",
		"/tmp/redis1.sock=redis://localhost:27127/0?poolsize=5&label=cluster1",
	}

	resetFlags()
	c, err := parseFlags()
	assert.Nil(t, err)

	assert.Equal(t, "before_env_value_after", c.statsd)
}

func TestInvalidLogLevel(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redis-proxy",
		"-loglevel", "wrong",
		"/tmp/redis1.sock=redis://localhost:27127/0?poolsize=5&label=cluster1",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "invalid loglevel: wrong")
}

func TestInvalidNetwork(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redis-proxy",
		"-network", "wrong",
		"/tmp/redis1.sock=redis://localhost:27127/0?poolsize=5&label=cluster1",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "invalid network: wrong")
}

func TestAddressCollision(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redis-proxy",
		"/tmp/redis1.sock=mongodb://localhost:27127/0?poolsize=5&label=cluster1",
		"/tmp/redis1.sock=mongodb://localhost:27128/0?poolsize=10&label=cluster2",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "duplicate entry for address: /tmp/redis1.sock")
}

func TestMissingAddresses(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redis-cluster",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "missing host=uri(s)")
}
