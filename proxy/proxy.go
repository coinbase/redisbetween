package proxy

import (
	"context"
	"fmt"
	"github.cbhq.net/engineering/redis-proxy/config"
	"github.cbhq.net/engineering/redis-proxy/elasticache"
	"github.cbhq.net/engineering/redis-proxy/handlers"
	"github.cbhq.net/engineering/redis-proxy/listener"
	"github.cbhq.net/engineering/redis-proxy/redis"
	"github.cbhq.net/engineering/redis-proxy/util"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/mediocregopher/radix/v3"
	"runtime/debug"
	"strings"
	"sync/atomic"

	"net"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

const restartSleep = 1 * time.Second
const disconnectTimeout = 10 * time.Second

var globalConnectionID uint64 = 1

func NextConnectionID() uint64 { return atomic.AddUint64(&globalConnectionID, 1) }

type Proxy struct {
	log    *zap.Logger
	statsd *statsd.Client

	config *config.Config

	network            string
	upstreamConfigHost string
	localConfigHost    string
	unlink             bool
	maxPoolSize        int
	minPoolSize        int
	cluster            bool
	topology           radix.ClusterTopo

	quit          chan interface{}
	kill          chan interface{}
	clusterEvents chan interface{}

	listeners []*listener.Listener
}

func NewProxy(log *zap.Logger, sd *statsd.Client, config *config.Config, label, localHost, upstreamHost string, cluster bool, minPoolSize, maxPoolSize int) (*Proxy, error) {
	if label != "" {
		log = log.With(zap.String("cluster", label))

		var err error
		sd, err = util.StatsdWithTags(sd, []string{fmt.Sprintf("cluster:%s", label)})
		if err != nil {
			return nil, err
		}
	}
	return &Proxy{
		log:    log,
		statsd: sd,
		config: config,

		upstreamConfigHost: upstreamHost,
		localConfigHost:    localHost,
		minPoolSize:        minPoolSize,
		maxPoolSize:        maxPoolSize,
		cluster:            cluster,

		quit:          make(chan interface{}),
		kill:          make(chan interface{}),
		clusterEvents: make(chan interface{}),
	}, nil
}

func (p *Proxy) Run() error {
	return p.run()
}

func (p *Proxy) Shutdown() {
	defer func() {
		_ = recover() // "close of closed channel" panic if Shutdown() was already called
	}()
	for _, l := range p.listeners {
		l.Shutdown()
	}
	close(p.quit)
}

func (p *Proxy) Kill() {
	p.Shutdown()
	defer func() {
		_ = recover() // "close of closed channel" panic if Kill() was already called
	}()
	for _, l := range p.listeners {
		l.Kill()
	}
	close(p.kill)
}

func (p *Proxy) run() error {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("Crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))

			time.Sleep(restartSleep)

			p.log.Info("Restarting", zap.Duration("sleep", restartSleep))
			go func() {
				err := p.run()
				if err != nil {
					p.log.Error("Error restarting", zap.Error(err))
				}
			}()
		}
	}()

	var listeners []*listener.Listener
	if p.cluster {
		topo, err := elasticache.ClusterNodes(p.log, p.upstreamConfigHost)
		if err != nil {
			return err
		}
		p.topology = topo

		m := topo.Map()
		upstreams := make([]string, 0)
		for a, _ := range m {
			upstreams = append(upstreams, a)
		}

		log.Info("discovered cluster nodes", zap.Strings("upstreams", upstreams))

		listeners, err = p.createClusterListeners(upstreams)
		if err != nil {
			return err
		}

		// TODO listen on the cluster events channel for topo change events. this will be
		// unused for non-clustered deployments.

	} else {
		// For non-clustered redis deployments, the config host points to the primary. At
		// the moment, we do not perform secondary reads.
		l, err := p.createListener(p.localConfigHost, p.upstreamConfigHost, p.minPoolSize, p.maxPoolSize)
		if err != nil {
			return err
		}
		listeners = []*listener.Listener{l}
	}

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	p.listeners = listeners

	for _, l := range listeners {
		l := l
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := l.Run()
			if err != nil {
				log.Error("Error", zap.Error(err))
			}
		}()
	}

	return nil
}

func (p *Proxy) createClusterListeners(upstreams []string) ([]*listener.Listener, error) {
	var listeners []*listener.Listener

	// Create a single connection to the discovery endpoint (which is a FQDN) mapped
	// to the localConfigHost address. This connects to one of the cluster primaries.
	// We create this redundant connection to allow the initial node discovery to
	// happen normally. Clients will initially connect to this handler and run
	// CLUSTER SLOTS, which will return a list of IPs. Each IP will get proxied to a
	// local socket, and all subsequent client traffic will occur via those.
	l, err := p.createListener(p.localConfigHost, p.upstreamConfigHost, 1, 1)
	if err != nil {
		return nil, err
	}
	listeners = append(listeners, l)

	for _, upstream := range upstreams {
		var local string
		i := NextConnectionID()
		if strings.Contains(p.config.Network, "unix") {
			local = fmt.Sprintf("%s%d%s", p.config.LocalSocketPrefix, i, p.config.LocalSocketSuffix)
		} else {
			port := uint64(p.config.LocalPortStart) + i
			local = fmt.Sprintf(":%d", port)
		}

		l, err := p.createListener(local, upstream, p.minPoolSize, p.maxPoolSize)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, l)
	}

	return listeners, nil
}

func (p *Proxy) createListener(local, upstream string, minPoolSize, maxPoolSize int) (*listener.Listener, error) {
	logWith := p.log.With(zap.String("upstream", upstream), zap.String("local", local))
	sdWith, err := util.StatsdWithTags(p.statsd, []string{fmt.Sprintf("upstream:%s", upstream), fmt.Sprintf("local:%s", local)})
	if err != nil {
		return nil, err
	}
	m, err := redis.ConnectServer(
		redis.Address(upstream),
		redis.WithMinConnections(func(uint64) uint64 { return uint64(minPoolSize) }),
		redis.WithMaxConnections(func(uint64) uint64 { return uint64(maxPoolSize) }),
		// TODO hook up the pool monitor
		//redis.WithConnectionPoolMonitor(func(*redis.PoolMonitor) *redis.PoolMonitor { return poolMonitor(sdWith) }),
	)
	if err != nil {
		return nil, err
	}

	connectionHandler := func(log *zap.Logger, conn net.Conn, id uint64, kill chan interface{}, events chan interface{}) {
		handlers.CommandConnection(log, p.statsd, p.config, conn, local, id, m, kill, events)
	}
	shutdownHandler := func() {
		ctx, cancel := context.WithTimeout(context.Background(), disconnectTimeout)
		defer cancel()
		_ = m.Disconnect(ctx)
	}
	return listener.New(logWith, sdWith, p.config, p.config.Network, local, connectionHandler, shutdownHandler, p.clusterEvents)
}
