package proxy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coinbase/memcachedbetween/listener"
	"github.com/coinbase/memcachedbetween/pool"
	"github.com/coinbase/mongobetween/util"
	"github.com/coinbase/redisbetween/config"
	"github.com/coinbase/redisbetween/handlers"
	"github.com/coinbase/redisbetween/redis"
	"github.com/mediocregopher/radix/v3"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

const restartSleep = 1 * time.Second
const disconnectTimeout = 10 * time.Second
const ping = "*1\r\n$4\r\nPING\r\n"
const pong = "PONG"

type Proxy struct {
	log    *zap.Logger
	statsd *statsd.Client

	config *config.Config

	upstreamConfigHost string
	localConfigHost    string
	maxPoolSize        int
	minPoolSize        int
	readTimeout        time.Duration
	writeTimeout       time.Duration
	database           int
	readonly           bool

	quit chan interface{}
	kill chan interface{}

	listeners    map[string]*listener.Listener
	listenerLock sync.Mutex
	listenerWg   sync.WaitGroup

	reservations *handlers.Reservations
}

func NewProxy(log *zap.Logger, sd *statsd.Client, config *config.Config, label, upstreamHost string, database int, minPoolSize, maxPoolSize int, readTimeout, writeTimeout time.Duration, readonly bool, maxSub, maxBlk int) (*Proxy, error) {
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
		localConfigHost:    localSocketPathFromUpstream(upstreamHost, database, readonly, config.LocalSocketPrefix, config.LocalSocketSuffix),
		minPoolSize:        minPoolSize,
		maxPoolSize:        maxPoolSize,
		readTimeout:        readTimeout,
		writeTimeout:       writeTimeout,
		database:           database,
		readonly:           readonly,

		quit: make(chan interface{}),
		kill: make(chan interface{}),

		listeners:    make(map[string]*listener.Listener),
		reservations: handlers.NewReservations(maxSub, maxBlk, sd),
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
	for _, ls := range p.listeners {
		ls.Shutdown()
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
	for _, ls := range p.listeners {
		ls.Kill()
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

	ls, err := p.createListener(p.localConfigHost, p.upstreamConfigHost)
	if err != nil {
		return err
	} else {
		p.log.Info("Created Listener", zap.String("localHost", p.localConfigHost), zap.String("upstreamHost", p.upstreamConfigHost))
	}
	defer func() {
		p.listenerWg.Wait()
	}()

	p.listenerLock.Lock()
	p.listeners[p.localConfigHost] = ls
	for _, ls = range p.listeners {
		p.runListener(ls)
	}
	p.listenerLock.Unlock()

	if p.config.HealthCheck {
		go p.healthCheckConnections()
	}
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
	p.log.Info("ensuring we have a listener for", zap.String("upstream", upstream), zap.String("command", originalCmd))
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()
	local := localSocketPathFromUpstream(upstream, p.database, p.readonly, p.config.LocalSocketPrefix, p.config.LocalSocketSuffix)
	_, ok := p.listeners[local]
	if !ok {
		p.log.Info("did not find listener, creating new one", zap.String("upstream", upstream), zap.String("local", local), zap.String("command", originalCmd))
		ls, err := p.createListener(local, upstream)
		if err != nil {
			p.log.Error("unable to create listener", zap.Error(err))
		}
		p.listeners[local] = ls
		p.runListener(ls)
	}
}

func (p *Proxy) createListener(local, upstream string) (*listener.Listener, error) {
	logWith := p.log.With(zap.String("upstream", upstream), zap.String("local", local))
	sdWith, err := util.StatsdWithTags(p.statsd, []string{fmt.Sprintf("upstream:%s", upstream), fmt.Sprintf("local:%s", local)})
	if err != nil {
		return nil, err
	}
	opts := []pool.ServerOption{
		pool.WithMinConnections(func(uint64) uint64 { return uint64(p.minPoolSize) }),
		pool.WithMaxConnections(func(uint64) uint64 { return uint64(p.maxPoolSize) }),
		pool.WithConnectionPoolMonitor(func(*pool.Monitor) *pool.Monitor { return poolMonitor(sdWith) }),
	}

	var initCommand []byte

	if p.database > -1 {
		// if a db number has been specified, we need to issue a SELECT command before adding
		// that connection to the pool, so it's always pinned to the right db
		d := strconv.Itoa(p.database)
		initCommand = []byte("*2\r\n$6\r\nSELECT\r\n$" + strconv.Itoa(len(d)) + "\r\n" + d + "\r\n")
	} else if p.readonly {
		// if this pool is designated for replica reads, we need to set the READONLY flag on
		// the upstream connection before adding it to the pool. this is only supported by
		// clustered redis, so it cannot be combined with SELECT.
		initCommand = []byte("*1\r\n$8\r\nREADONLY\r\n")
	}

	if initCommand != nil {
		co := connectWithInitCommand(initCommand, logWith)
		opts = append(opts, pool.WithConnectionOptions(func(cos ...pool.ConnectionOption) []pool.ConnectionOption {
			return append(cos, co)
		}))
	}

	s, err := pool.ConnectServer(pool.Address(upstream), opts...)
	if err != nil {
		return nil, err
	}

	connectionHandler := func(log *zap.Logger, conn net.Conn, id uint64, kill chan interface{}) {
		handlers.CommandConnection(log, p.statsd, conn, local, p.readTimeout, p.writeTimeout, id, s, kill, p.quit, p.interceptMessages, p.reservations)
	}
	shutdownHandler := func() {
		ctx, cancel := context.WithTimeout(context.Background(), disconnectTimeout)
		defer cancel()
		_ = s.Disconnect(ctx)
	}

	listener, err := listener.New(logWith, sdWith, p.config.Network, local, p.config.Unlink, connectionHandler, shutdownHandler)
	if err != nil {
		return nil, err
	}
	return listener, nil
}

// Go through all listeners and health check their servers
// This should ideally by its own class with a healthcheck strategy
// Due to timeline pressures we'll just use this basic scheme
// to avoid repeated loops of Timeout on the redis client
func (p *Proxy) healthCheckConnections() {
	duration := time.Duration(p.config.ServerHealthCheckSec) * time.Second
	p.log.Debug("Inside healthCheckConnections", zap.String("duration", duration.String()))
	for {
		time.Sleep(duration)
		p.log.Debug("Just woke up to healthcheck connections")
		keys := p.getListenerKeys()
		var wg sync.WaitGroup
		for _, key := range keys {
			wg.Add(1)
			go p.healthCheckSingleConnection(key, &wg)
		}
		wg.Wait()
	}
}

// Health check a single host:port
// Process:
// - check out a connection from the server
// - send a ping command
// - if error, repeat for x times
// - if kept on erroring:
//   - replace listener if the upstreamConfigHost
//   - remove listener if otherwise
//
// If the server repeatedly fails remove them unless they are the main upstream host
// In that case, just try to recreate the connections
// The listeners that get removed will be re-created only during the intercepts of
// CLUSTER NODES commands sent by the client
func (p *Proxy) healthCheckSingleConnection(key string, wg *sync.WaitGroup) {
	p.log.Debug("Inside healthCheckSingleConnection", zap.String("server", key))
	defer wg.Done()

	healthy := true
	for i := 0; i < int(p.config.ServerHealthCheckThreshold); i++ {
		time.Sleep(1 * time.Second)
		healthy = pingServer(p.config.Network, key, p.readTimeout, p.writeTimeout, p.log)
		p.log.Debug("Finished pinging server", zap.String("server", key), zap.String("healthy", strconv.FormatBool(healthy)))
		if healthy {
			break
		}
	}
	if !healthy {
		p.log.Warn("Server failed to respond; Deleting the listener", zap.String("server", key))
		p.deleteListener(key)
		p.getListener(key).Shutdown()
		if key == p.localConfigHost {
			// add the upstream config host back; we always need to have that minimally
			// but hopefully this time, the connection is re-established to the right IP
			p.ensureListenerForUpstream(key, "")
		}
	}
}

// Safely grab an entry for a given key from the listeners map
func (p *Proxy) getListener(key string) *listener.Listener {
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()
	ls, ok := p.listeners[key]
	if ok {
		return ls
	}
	return nil
}

// Safely delete a listener from the map
func (p *Proxy) deleteListener(key string) {
	p.listenerLock.Lock()
	delete(p.listeners, key)
	p.listenerLock.Unlock()
}

// Get a range of keys in the listeners collection
func (p *Proxy) getListenerKeys() []string {
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()
	var keys []string
	for key, _ := range p.listeners {
		keys = append(keys, key)
	}
	return keys
}

func connectWithInitCommand(command []byte, logWith *zap.Logger) pool.ConnectionOption {
	co := pool.WithDialer(func(dialer pool.Dialer) pool.Dialer {
		return pool.DialerFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
			dlr := &net.Dialer{Timeout: 30 * time.Second}
			conn, err := dlr.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			_, err = conn.Write(command)
			if err != nil {
				logWith.Error("failed to write command", zap.Error(err))
				return nil, err
			}
			res := make([]byte, 5)
			_, err = io.ReadFull(conn, res)
			if err != nil || string(res) != "+OK\r\n" {
				logWith.Error("failed to read response", zap.Error(err), zap.String("response", string(res)))
				return nil, err
			}
			return conn, err
		})
	})
	return co
}

func poolMonitor(sd *statsd.Client) *pool.Monitor {
	checkedOut, checkedIn := util.StatsdBackgroundGauge(sd, "pool.checked_out_connections", []string{})
	opened, closed := util.StatsdBackgroundGauge(sd, "pool.open_connections", []string{})

	return &pool.Monitor{
		Event: func(e *pool.Event) {
			snake := strings.ToLower(regexp.MustCompile("([a-z0-9])([A-Z])").ReplaceAllString(e.Type, "${1}_${2}"))
			name := fmt.Sprintf("pool_event.%s", snake)
			tags := []string{
				fmt.Sprintf("address:%s", e.Address),
				fmt.Sprintf("reason:%s", e.Reason),
			}
			switch e.Type {
			case pool.ConnectionCreated:
				opened(name, tags)
			case pool.ConnectionClosed:
				closed(name, tags)
			case pool.GetSucceeded:
				checkedOut(name, tags)
			case pool.ConnectionReturned:
				checkedIn(name, tags)
			default:
				_ = sd.Incr(name, tags, 1)
			}
		},
	}
}

// Use the redis PING command (response: PONG) to determine if the connection is healthy
func pingServer(network, address string, readTimeout, writeTimeout time.Duration, logger *zap.Logger) bool {
	dlr := &net.Dialer{Timeout: 30 * time.Second}
	conn, err := dlr.DialContext(context.Background(), network, address)
	if err != nil {
		if logger != nil {
			logger.Error("failed to open local address", zap.String("local", address), zap.Error(err))
		}
		return false
	}
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err = conn.Write([]byte(ping))
	if err != nil {
		if logger != nil {
			logger.Error("failed to write PING", zap.Error(err))
		}
		return false
	}
	conn.SetReadDeadline(time.Now().Add(readTimeout))
	resp := make([]byte, 7)
	_, err = io.ReadFull(conn, resp)
	if err != nil || !strings.Contains(string(resp), pong) {
		if logger != nil {
			logger.Error("expected PONG in response but got error", zap.String("response", string(resp)), zap.Error(err))
		}
		return false
	}
	return true
}
