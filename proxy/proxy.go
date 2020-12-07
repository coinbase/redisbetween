package proxy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.cbhq.net/engineering/redisbetween/config"
	"github.cbhq.net/engineering/redisbetween/handlers"
	"github.cbhq.net/engineering/redisbetween/listener"
	"github.cbhq.net/engineering/redisbetween/redis"
	"github.cbhq.net/engineering/redisbetween/util"
	"github.com/mediocregopher/radix/v3"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

const restartSleep = 1 * time.Second
const disconnectTimeout = 10 * time.Second

type Proxy struct {
	log    *zap.Logger
	statsd *statsd.Client

	config *config.Config

	upstreamConfigHost string
	localConfigHost    string
	maxPoolSize        int
	minPoolSize        int
	cluster            bool
	database           int

	quit chan interface{}
	kill chan interface{}

	listeners    map[string]*listener.Listener
	listenerLock sync.Mutex
	listenerWg   sync.WaitGroup
}

func NewProxy(log *zap.Logger, sd *statsd.Client, config *config.Config, label, upstreamHost string, database int, cluster bool, minPoolSize, maxPoolSize int) (*Proxy, error) {
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
		localConfigHost:    localSocketPathFromUpstream(upstreamHost, database, config.LocalSocketPrefix, config.LocalSocketSuffix),
		minPoolSize:        minPoolSize,
		maxPoolSize:        maxPoolSize,
		cluster:            cluster,
		database:           database,

		quit: make(chan interface{}),
		kill: make(chan interface{}),

		listeners: make(map[string]*listener.Listener),
	}, nil
}

func (p *Proxy) Run() error {
	return p.run()
}

func (p *Proxy) Shutdown() {
	defer func() {
		_ = recover() // "close of closed channel" panic if Shutdown() was already called
	}()
	p.listenerLock.Lock()
	for _, l := range p.listeners {
		l.Shutdown()
	}
	p.listenerLock.Unlock()
	close(p.quit)
}

func (p *Proxy) Kill() {
	p.Shutdown()
	defer func() {
		_ = recover() // "close of closed channel" panic if Kill() was already called
	}()
	p.listenerLock.Lock()
	for _, l := range p.listeners {
		l.Kill()
	}
	p.listenerLock.Unlock()
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

	l, err := p.createListener(p.localConfigHost, p.upstreamConfigHost)
	if err != nil {
		return err
	}
	defer func() {
		p.listenerWg.Wait()
	}()

	p.listenerLock.Lock()
	p.listeners[p.upstreamConfigHost] = l
	for _, l := range p.listeners {
		p.runListener(l)
	}
	p.listenerLock.Unlock()

	return nil
}

func (p *Proxy) runListener(l *listener.Listener) {
	p.listenerWg.Add(1)
	go func() {
		defer p.listenerWg.Done()

		err := l.Run()
		if err != nil {
			p.log.Error("Error", zap.Error(err))
		}
	}()
}

func (p *Proxy) interceptMessage(originalCmd string, m *redis.Message) {
	if !p.cluster {
		return
	}

	if originalCmd == "CLUSTER SLOTS" {
		b, err := redis.EncodeToBytes(m)
		if err != nil {
			return
		}
		slots := radix.ClusterTopo{}
		err = slots.UnmarshalRESP(bufio.NewReader(bytes.NewReader(b)))
		if err != nil {
			return
		}
		for _, slot := range slots {
			p.ensureListenerForUpstream(slot.Addr)
		}
		return
	}

	if originalCmd == "CLUSTER NODES" {
		if m.IsBulkBytes() {
			lines := strings.Split(string(m.Value), "\n")
			for _, line := range lines {
				lt := strings.IndexByte(line, ' ')
				rt := strings.IndexByte(line, '@')
				if lt > 0 && rt > 0 {
					hostPort := line[lt+1 : rt]
					p.ensureListenerForUpstream(hostPort)
				}
			}
		}
	}

	if m.IsError() {
		msg := string(m.Value)
		if strings.HasPrefix(msg, "MOVED") || strings.HasPrefix(msg, "ASK") {
			parts := strings.Split(msg, " ")
			if len(parts) < 3 {
				p.log.Error("failed to parse MOVED error", zap.String("original command", originalCmd), zap.String("original message", msg))
				return
			}
			p.ensureListenerForUpstream(parts[2])
		}
	}
}

func localSocketPathFromUpstream(upstream string, database int, prefix, suffix string) string {
	hostPort := strings.Split(upstream, ":")
	path := prefix + hostPort[0] + "-" + hostPort[1]
	if database > -1 {
		path += "-" + strconv.Itoa(database)
	}
	return path + suffix
}

func (p *Proxy) ensureListenerForUpstream(upstream string) {
	p.log.Info("ensuring we have a listener for", zap.String("upstream", upstream))
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()
	_, ok := p.listeners[upstream]
	if !ok {
		local := localSocketPathFromUpstream(upstream, p.database, p.config.LocalSocketPrefix, p.config.LocalSocketSuffix)
		p.log.Info("did not find listener, creating new one", zap.String("upstream", upstream), zap.String("local", local))
		l, err := p.createListener(local, upstream)
		if err != nil {
			p.log.Error("unable to create listener", zap.Error(err))
		}
		p.listeners[upstream] = l
		p.runListener(l)
	}
}

func (p *Proxy) createListener(local, upstream string) (*listener.Listener, error) {
	logWith := p.log.With(zap.String("upstream", upstream), zap.String("local", local))
	sdWith, err := util.StatsdWithTags(p.statsd, []string{fmt.Sprintf("upstream:%s", upstream), fmt.Sprintf("local:%s", local)})
	if err != nil {
		return nil, err
	}
	m, err := redis.ConnectServer(
		redis.Address(upstream),
		redis.WithMinConnections(func(uint64) uint64 { return uint64(p.minPoolSize) }),
		redis.WithMaxConnections(func(uint64) uint64 { return uint64(p.maxPoolSize) }),
		// TODO hook up the pool monitor
		//redis.WithConnectionPoolMonitor(func(*redis.PoolMonitor) *redis.PoolMonitor { return poolMonitor(sdWith) }),
	)
	if err != nil {
		return nil, err
	}

	connectionHandler := func(log *zap.Logger, conn net.Conn, id uint64, kill chan interface{}) {
		handlers.CommandConnection(log, p.statsd, p.config, conn, local, id, m, kill, p.interceptMessage)
	}
	shutdownHandler := func() {
		ctx, cancel := context.WithTimeout(context.Background(), disconnectTimeout)
		defer cancel()
		_ = m.Disconnect(ctx)
	}

	// TODO send the SELECT command if p.database > -1

	return listener.New(logWith, sdWith, p.config, p.config.Network, local, connectionHandler, shutdownHandler)
}
