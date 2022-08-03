package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/coinbase/redisbetween/utils"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coinbase/memcachedbetween/listener"
	"github.com/coinbase/mongobetween/util"
	"github.com/coinbase/redisbetween/config"
	"github.com/coinbase/redisbetween/handlers"
	"github.com/coinbase/redisbetween/redis"
	"github.com/mediocregopher/radix/v3"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

const restartSleep = 1 * time.Second

type Proxy struct {
	log            *zap.Logger
	statsd         *statsd.Client
	listenerConfig *config.Listener
	upstreamConfig *config.Upstream
	upstreams      UpstreamManager
	localHost      string
	quit           chan interface{}
	kill           chan interface{}
	listenerLock   sync.Mutex
	listenerWg     sync.WaitGroup
	listeners      map[string]*listener.Listener
	reservations   *handlers.Reservations
}

func NewProxy(ctx context.Context, listenerConfig *config.Listener, upstreams UpstreamManager) (*Proxy, error) {
	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	sd := ctx.Value(utils.CtxStatsdKey).(*statsd.Client)

	// Make a local copy of the config
	listenerCfg := &config.Listener{}
	*listenerCfg = *listenerConfig

	if listenerCfg.Name != "" {
		log = log.With(zap.String("cluster", listenerCfg.Name))

		var err error
		sd, err = util.StatsdWithTags(sd, []string{fmt.Sprintf("cluster:%s", listenerCfg.Name)})
		if err != nil {
			return nil, err
		}
	}

	// Make a local copy of the config to use for future listener/upstream creation due
	// to cluster command intercept.
	upstreamCfg := &config.Upstream{}
	if u, ok := upstreams.ConfigByName(ctx, listenerCfg.Target); ok {
		*upstreamCfg = *u
	} else {
		log.Error("Missing upstream in config", zap.String("target", listenerCfg.Target))
		return nil, errors.New("MISSING_UPSTREAM")
	}

	localHost := localSocketPathFromUpstream(upstreamCfg.Address, upstreamCfg.Database, upstreamCfg.Readonly, listenerCfg.LocalSocketPrefix, listenerCfg.LocalSocketSuffix)

	return &Proxy{
		log:            log,
		statsd:         sd,
		listenerConfig: listenerCfg,
		upstreamConfig: upstreamCfg,
		upstreams:      upstreams,
		localHost:      localHost,
		quit:           make(chan interface{}),
		kill:           make(chan interface{}),

		listeners:    make(map[string]*listener.Listener),
		reservations: handlers.NewReservations(listenerConfig.MaxSubscriptions, listenerConfig.MaxBlockers, sd),
	}, nil
}

func (p *Proxy) Run(ctx context.Context) error {
	return p.run(ctx)
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

	p.reservations.Close()
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

func (p *Proxy) run(ctx context.Context) error {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("Crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))

			time.Sleep(restartSleep)

			p.log.Info("Restarting", zap.Duration("sleep", restartSleep))
			go func() {
				err := p.run(ctx)
				if err != nil {
					p.log.Error("Error restarting", zap.Error(err))
				}
			}()
		}
	}()

	l, err := p.createListener(p.localHost, p.listenerConfig.Target)
	if err != nil {
		return err
	}
	defer func() {
		p.listenerWg.Wait()
	}()

	p.listenerLock.Lock()
	p.listeners[p.upstreamConfig.Address] = l
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

func (p *Proxy) interceptMessages(originalCmds []string, mm []*redis.Message) {
	for i, m := range mm {
		if originalCmds[i] == "CLUSTER SLOTS" {
			b, err := redis.EncodeToBytes(m)
			if err != nil {
				p.log.Error("failed to encode cluster slots message", zap.Error(err))
				return
			}
			slots := radix.ClusterTopo{}
			err = slots.UnmarshalRESP(bufio.NewReader(bytes.NewReader(b)))
			if err != nil {
				p.log.Error("failed to unmarshal cluster slots message", zap.Error(err))
				return
			}
			for _, slot := range slots {
				p.ensureListenerForUpstream(slot.Addr, originalCmds[i])
			}
			return
		}

		if originalCmds[i] == "CLUSTER NODES" {
			if m.IsBulkBytes() {
				lines := strings.Split(string(m.Value), "\n")
				for _, line := range lines {
					lt := strings.IndexByte(line, ' ')
					rt := strings.IndexByte(line, '@')
					if lt > 0 && rt > 0 {
						hostPort := line[lt+1 : rt]
						p.ensureListenerForUpstream(hostPort, originalCmds[i])
					}
				}
			}
		}

		if m.IsError() {
			msg := string(m.Value)
			if strings.HasPrefix(msg, "MOVED") || strings.HasPrefix(msg, "ASK") {
				parts := strings.Split(msg, " ")
				if len(parts) < 3 {
					p.log.Error("failed to parse MOVED error", zap.String("original command", originalCmds[i]), zap.String("original message", msg))
					return
				}
				p.ensureListenerForUpstream(parts[2], originalCmds[i]+" "+parts[0])
			}
		}
	}
}

func localSocketPathFromUpstream(upstream string, database int, readonly bool, prefix, suffix string) string {
	path := prefix + strings.Replace(upstream, ":", "-", -1)
	if database > -1 {
		path += "-" + strconv.Itoa(database)
	}
	if readonly {
		path += "-ro"
	}
	return path + suffix
}

func (p *Proxy) ensureListenerForUpstream(upstream, originalCmd string) {
	log := p.log.With(zap.String("upstream", upstream), zap.String("command", originalCmd))
	log.Info("adding upstream to manager")
	upstreamCfg := *p.upstreamConfig
	upstreamCfg.Name = upstream
	upstreamCfg.Address = upstream
	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxStatsdKey, p.statsd), utils.CtxLogKey, p.log)
	_ = p.upstreams.Add(ctx, &upstreamCfg)

	log.Info("ensuring we have a listener for")
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()
	_, ok := p.listeners[upstream]
	if !ok {
		local := localSocketPathFromUpstream(upstream, p.upstreamConfig.Database, p.upstreamConfig.Readonly, p.listenerConfig.LocalSocketPrefix, p.listenerConfig.LocalSocketSuffix)
		log.Info("did not find listener, creating new one", zap.String("local", local))
		l, err := p.createListener(local, upstream)
		if err != nil {
			log.Error("unable to create listener", zap.Error(err))
			return
		}
		p.listeners[upstream] = l
		p.runListener(l)
	}
}

func (p *Proxy) createListener(local, upstream string) (*listener.Listener, error) {
	logWith := p.log.With(zap.String("local", local))
	sdWith, err := util.StatsdWithTags(p.statsd, []string{fmt.Sprintf("upstream:%s", upstream), fmt.Sprintf("local:%s", local)})
	if err != nil {
		return nil, err
	}

	connectionHandler := func(log *zap.Logger, conn net.Conn, id uint64, kill chan interface{}) {
		handlers.CommandConnection(log, p.statsd, conn, local, upstream, id, kill, p.quit, p.interceptMessages, p.reservations, p.upstreams, p.listenerConfig)
	}

	return listener.New(logWith, sdWith, p.listenerConfig.Network, local, p.listenerConfig.Unlink, connectionHandler, func() {})
}

func (p *Proxy) Update(l *config.Listener) error {
	p.listenerConfig.Mirroring = l.Mirroring
	p.listenerConfig.Target = l.Target

	return nil
}
