package config

import (
	"flag"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"os"
	"testing"
	"time"
)

func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
}

func TestParseFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redisbetween",
		"-loglevel", "debug",
		"-pretty",
		"-statsd", "statsd:1234",
		"-readtimeout", "1s",
		"-writetimeout", "1s",
		"-url", "/tmp/redisbetween/config.json",
		"-pollinterval", "10",
	}

	minPoolEnvVar := "TestParseFlags_MinPoolSize"
	_ = os.Setenv(minPoolEnvVar, "10")
	defer func() {
		_ = os.Unsetenv(minPoolEnvVar)
	}()

	resetFlags()
	c, err := ParseFlags()
	fmt.Println(err)
	assert.NoError(t, err)

	assert.Equal(t, "statsd:1234", c.Statsd)
	assert.Equal(t, zapcore.DebugLevel, c.Level)
	assert.Equal(t, 10*time.Second, c.PollInterval)
	assert.Equal(t, "/tmp/redisbetween/config.json", c.ConfigUrl)
}

func TestInvalidLogLevel(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redisbetween",
		"-loglevel", "wrong",
		"-url", "./temp",
	}

	resetFlags()
	_, err := ParseFlags()
	assert.EqualError(t, err, "invalid loglevel: wrong")
}

func TestMissingConfigUrl(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"redis-cluster",
	}

	resetFlags()
	_, err := ParseFlags()
	assert.EqualError(t, err, "-url must be present")
}
