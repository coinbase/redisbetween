package config

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
}

func TestParseFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redisbetween",
		"-localsocketprefix", "/some/path/redisbetween-",
		"-localsocketsuffix", ".socket",
		"-loglevel", "debug",
		"-network", "unix",
		"-pretty",
		"-statsd", "statsd:1234",
		"-unlink",
		"-readtimeout", "1s",
		"-writetimeout", "1s",
		"redis://localhost:7000/0?minpoolsize=5&maxpoolsize=33&label=cluster1",
		"redis://localhost:7002?minpoolsize=10&label=cluster2&readtimeout=3s&writetimeout=6s",
	}

	resetFlags()
	c, err := parseFlags()
	fmt.Println(err)
	assert.NoError(t, err)

	assert.Equal(t, "statsd:1234", c.Statsd)
	assert.Equal(t, zapcore.DebugLevel, c.Level)
	assert.Equal(t, "unix", c.Network)
	assert.True(t, c.Unlink)

	assert.Equal(t, 2, len(c.Upstreams))
	upstream1 := c.Upstreams[0]
	upstream2 := c.Upstreams[1]
	if upstream1.Label == "cluster2" {
		temp := upstream1
		upstream1 = upstream2
		upstream2 = temp
	}

	assert.Equal(t, "cluster1", upstream1.Label)
	assert.Equal(t, "localhost:7000", upstream1.UpstreamConfigHost)
	assert.Equal(t, 5, upstream1.MinPoolSize)
	assert.Equal(t, 0, upstream1.Database)
	assert.Equal(t, 5*time.Second, upstream1.ReadTimeout)
	assert.Equal(t, 5*time.Second, upstream1.WriteTimeout)

	assert.Equal(t, "cluster2", upstream2.Label)
	assert.Equal(t, "localhost:7002", upstream2.UpstreamConfigHost)
	assert.Equal(t, 10, upstream2.MinPoolSize)
	assert.Equal(t, 3*time.Second, upstream2.ReadTimeout)
	assert.Equal(t, 6*time.Second, upstream2.WriteTimeout)
}

func TestInvalidLogLevel(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redisbetween",
		"-loglevel", "wrong",
		"redis://localhost?minpoolsize=5&label=cluster1",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "invalid loglevel: wrong")
}

func TestInvalidNetwork(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redisbetween",
		"-network", "wrong",
		"redis://localhost?minpoolsize=5&label=cluster1",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "invalid network: wrong")
}

func TestAddressCollision(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redisbetween",
		"redis://localhost?minpoolsize=5&label=cluster1",
		"redis://localhost?minpoolsize=10&label=cluster2",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "duplicate entry for address: localhost")
}

func TestMissingAddresses(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redis-cluster",
	}

	resetFlags()
	_, err := parseFlags()
	assert.EqualError(t, err, "missing list of upstream hosts")
}
