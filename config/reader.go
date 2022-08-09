package config

import "context"

func BuildFromOptions(_ context.Context, opts *Options) (Config, error) {
	var listeners []*Listener
	var upstreams []*Upstream

	for _, u := range opts.Upstreams {
		upstreams = append(upstreams, &Upstream{
			Name:         u.Label,
			Address:      u.UpstreamConfigHost,
			Database:     u.Database,
			MaxPoolSize:  u.MaxPoolSize,
			MinPoolSize:  u.MinPoolSize,
			ReadTimeout:  u.ReadTimeout,
			WriteTimeout: u.WriteTimeout,
			Readonly:     u.Readonly,
		})

		listeners = append(listeners, &Listener{
			Name:              u.Label,
			Network:           opts.Network,
			LocalSocketPrefix: opts.LocalSocketPrefix,
			LocalSocketSuffix: opts.LocalSocketSuffix,
			Target:            u.Label,
			MaxSubscriptions:  u.MaxSubscriptions,
			MaxBlockers:       u.MaxBlockers,
			Unlink:            opts.Unlink,
		})
	}

	return Config{
		Pretty:    opts.Pretty,
		Statsd:    opts.Statsd,
		Level:     opts.Level,
		Listeners: listeners,
		Upstreams: upstreams,
	}, nil
}
