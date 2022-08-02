package config

import (
	"context"
	"github.com/coinbase/redisbetween/utils"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const defaultStatsdAddress = "localhost:8125"

var validNetworks = []string{"tcp", "tcp4", "tcp6", "unix", "unixpacket"}

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
	LogLevel          zapcore.Level
	Target            string
	MaxSubscriptions  int
	MaxBlockers       int
	Unlink            bool
	Mirroring         *RequestMirrorPolicy
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

type RequestMirrorPolicy struct {
	Upstream string
}

func New(ctx context.Context, input *configAlias) *Config {
	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)

	logLevel := zap.InfoLevel
	if input.Level != "" {
		if err := logLevel.Set(input.Level); err != nil {
			log.Error("invalid log level for config", zap.Error(err))
		}
	}

	listeners := make([]*Listener, 0, len(input.Listeners))
	upstreams := make([]*Upstream, 0, len(input.Upstreams))

	for index, l := range input.Listeners {
		log := log.With(zap.Int("index", index), zap.String("field", "Listeners"))
		var mirroring *RequestMirrorPolicy

		if l.Target == "" {
			log.Error("listener must have target. skipping")
			continue
		}

		if !validNetwork(l.Network) {
			log.Error("invalid network specified for listener. skipping")
			continue
		}

		if l.Mirroring != nil && l.Mirroring.Upstream != "" {
			mirroring = &RequestMirrorPolicy{Upstream: l.Mirroring.Upstream}
		}

		if l.LocalSocketPrefix == "" {
			l.LocalSocketPrefix = "/var/tmp/redisbetween-"
		}

		if l.LocalSocketSuffix == "" {
			l.LocalSocketSuffix = ".sock"
		}

		if l.MaxSubscriptions < 1 {
			l.MaxSubscriptions = 1
		}

		if l.MaxBlockers < 1 {
			l.MaxBlockers = 1
		}

		listener := &Listener{
			Name:              l.Name,
			Network:           l.Network,
			LocalSocketPrefix: l.LocalSocketPrefix,
			LocalSocketSuffix: l.LocalSocketSuffix,
			Target:            l.Target,
			MaxSubscriptions:  l.MaxSubscriptions,
			MaxBlockers:       l.MaxBlockers,
			Unlink:            l.Unlink,
			LogLevel:          logLevel,
			Mirroring:         mirroring,
		}

		listeners = append(listeners, listener)
	}

	names := make(map[string]bool)
	for index, u := range input.Upstreams {
		log := log.With(zap.Int("index", index), zap.String("field", "Upstreams"))

		if u.Name == "" {
			log.Error("upstream must have a name")
			continue
		}

		if _, ok := names[u.Name]; ok {
			log.Error("duplicate upstream for the name present. skipping")
			continue
		}

		if u.Database < -1 {
			u.Database = -1
		}

		if u.MinPoolSize < 1 {
			u.MinPoolSize = 1
		}

		if u.MaxPoolSize < 1 {
			u.MaxPoolSize = 10
		}

		names[u.Name] = true

		upstream := &Upstream{
			Name:         u.Name,
			Address:      u.Address,
			Database:     u.Database,
			MaxPoolSize:  u.MaxPoolSize,
			MinPoolSize:  u.MinPoolSize,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
			Readonly:     u.Readonly,
		}

		t, err := time.ParseDuration(u.WriteTimeout)
		if err != nil {
			log.Error("failed to parse write timeout", zap.Error(err))
		} else {
			upstream.WriteTimeout = t
		}
		t, err = time.ParseDuration(u.ReadTimeout)
		if err != nil {
			log.Error("failed to parse read timeout", zap.Error(err))
		} else {
			upstream.ReadTimeout = t
		}

		upstreams = append(upstreams, upstream)
	}

	return &Config{
		Pretty:    input.Pretty,
		Statsd:    input.Statsd,
		Level:     logLevel,
		Listeners: listeners,
		Upstreams: upstreams,
	}
}

type configAlias struct {
	Pretty    bool
	Statsd    string
	Level     string
	Listeners []*struct {
		Name              string
		Network           string
		LocalSocketPrefix string
		LocalSocketSuffix string
		LogLevel          string
		Target            string
		MaxSubscriptions  int
		MaxBlockers       int
		Unlink            bool
		Mirroring         *struct{ Upstream string }
	}
	Upstreams []*struct {
		Name         string
		Address      string
		Database     int
		MaxPoolSize  int
		MinPoolSize  int
		ReadTimeout  string
		WriteTimeout string
		Readonly     bool
	}
}

func validNetwork(network string) bool {
	for _, n := range validNetworks {
		if n == network {
			return true
		}
	}
	return false
}
