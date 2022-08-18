package config

import (
	"time"

	"go.uber.org/zap/zapcore"
)

type Config struct {
	Pretty    bool
	Statsd    string
	Level     zapcore.Level
	Listeners []*Listener
	Upstreams []*Upstream
}

type Listener struct {
	Name              string
	Network           string
	LocalSocketPrefix string
	LocalSocketSuffix string
	Target            string
	MaxSubscriptions  int
	MaxBlockers       int
	Unlink            bool
}

type Upstream struct {
	Name         string
	Address      string
	Database     int
	MaxPoolSize  int
	MinPoolSize  int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Readonly     bool
}
