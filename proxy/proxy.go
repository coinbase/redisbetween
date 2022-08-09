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
	log    *zap.Logger
	statsd *statsd.Client

	config *config.Listener
	lookup UpstreamManager

	quit chan interface{}
	kill chan interface{}

	listeners      map[string]*listener.Listener
	listenerConfig map[string]*config.Listener
	listenerLock   sync.Mutex
	listenerWg     sync.WaitGroup

	reservations *handlers.Reservations
}

func NewProxy(ctx context.Context, listenerConfig *config.Listener, lookup UpstreamManager) (*Proxy, error) {
	cfg := &config.Listener{}
	*cfg = *listenerConfig

	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	sd := ctx.Value(utils.CtxStatsdKey).(*statsd.Client)

	if cfg.Name != "" {
		log = log.With(zap.String("cluster", cfg.Name))

		var err error
		sd, err = util.StatsdWithTags(sd, []string{fmt.Sprintf("cluster:%s", cfg.Name)})
		if err != nil {
			return nil, err
		}
	}

	return &Proxy{
		log:    log,
		statsd: sd,
		config: cfg,
		lookup: lookup,

		quit: make(chan interface{}),
		kill: make(chan interface{}),

		listeners:      make(map[string]*listener.Listener),
		listenerConfig: make(map[string]*config.Listener),
		reservations:   handlers.NewReservations(cfg.MaxSubscriptions, cfg.MaxBlockers, sd),
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

	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxLogKey, p.log), utils.CtxStatsdKey, p.statsd)
	upstreamConfig, ok := p.lookup.ConfigByName(ctx, p.config.Target)

	if !ok {
		return errors.New(fmt.Sprintf("Missing upstream config: %v", p.config.Target))
	}

	localSocket := localSocketPathFromUpstream(upstreamConfig.Address, upstreamConfig.Database, upstreamConfig.Readonly, p.config.LocalSocketPrefix, p.config.LocalSocketSuffix)
	l, err := NewListener(ctx, localSocket, p.config, p.lookup, p.interceptMessages, p.reservations, p.quit)
	if err != nil {
		return err
	}
	defer func() {
		p.listenerWg.Wait()
	}()

	p.listenerLock.Lock()
	p.listeners[localSocket] = l
	p.listenerConfig[localSocket] = p.config
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
	log.Info("ensuring we have a listener for")

	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()

	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxLogKey, log), utils.CtxStatsdKey, p.statsd)
	cfg, ok := p.lookup.ConfigByName(ctx, p.config.Target)

	if !ok {
		log.Error("failed to find upstream", zap.String("target", p.config.Target))
		return
	}

	upstreamConfig := &config.Upstream{}
	*upstreamConfig = *cfg
	upstreamConfig.Name = upstream
	upstreamConfig.Address = upstream

	err := p.lookup.Add(ctx, upstreamConfig)

	if err != nil {
		log.Error("failed to register upstream", zap.Error(err))
	}

	localSocket := localSocketPathFromUpstream(upstreamConfig.Address, upstreamConfig.Database, upstreamConfig.Readonly, p.config.LocalSocketPrefix, p.config.LocalSocketSuffix)
	_, ok = p.listeners[localSocket]
	if !ok {
		log.Info("did not find listener, creating new one", zap.String("local", localSocket))

		listenerConfig := &config.Listener{}
		*listenerConfig = *p.config
		listenerConfig.Name = upstreamConfig.Name
		listenerConfig.Target = upstreamConfig.Name

		l, err := NewListener(ctx, localSocket, listenerConfig, p.lookup, p.interceptMessages, p.reservations, p.quit)
		if err != nil {
			p.log.Error("unable to create listener", zap.Error(err))
		}
		p.listeners[localSocket] = l
		p.listenerConfig[localSocket] = listenerConfig
		p.runListener(l)
	}
}

func NewListener(ctx context.Context, localSocket string, cfg *config.Listener, lookup handlers.UpstreamLookup, interceptor handlers.MessageInterceptor, r *handlers.Reservations, quit chan interface{}) (*listener.Listener, error) {
	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	sd := ctx.Value(utils.CtxStatsdKey).(*statsd.Client)

	logWith := log.With(zap.String("upstream", cfg.Target), zap.String("local", localSocket))
	sdWith, err := util.StatsdWithTags(sd, []string{fmt.Sprintf("upstream:%s", cfg.Target), fmt.Sprintf("local:%s", localSocket)})

	if err != nil {
		return nil, err
	}

	connectionHandler := func(log *zap.Logger, conn net.Conn, id uint64, kill chan interface{}) {
		handlers.CommandConnection(log, sdWith, conn, localSocket, id, kill, quit, interceptor, r, lookup, cfg)
	}
	shutdownHandler := func() {}

	return listener.New(logWith, sdWith, cfg.Network, localSocket, cfg.Unlink, connectionHandler, shutdownHandler)
}
