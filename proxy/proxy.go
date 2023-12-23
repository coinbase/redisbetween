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
	"github.com/mediocregopher/radix/v3"

	"github.com/d2army/redisbetween/config"
	"github.com/d2army/redisbetween/handlers"
	"github.com/d2army/redisbetween/redis"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

const restartSleep = 1 * time.Second
const disconnectTimeout = 10 * time.Second
const ping = "*1\r\n$4\r\nPING\r\n"
const pong = "PONG"

type StatsdBackgroundGaugeCallback func(name string, tags []string)

type statsdCounters struct {
	listenerInc StatsdBackgroundGaugeCallback
	listenerDec StatsdBackgroundGaugeCallback
}

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
	idleTimeout        time.Duration

	quit chan interface{}
	kill chan interface{}

	listeners    map[string]*listener.Listener
	listenerLock sync.Mutex
	listenerWg   sync.WaitGroup

	reservations *handlers.Reservations
	statsdCounters
}

func NewProxy(log *zap.Logger, sd *statsd.Client, config *config.Config, upstreamIndex int) (*Proxy, error) {
	up := config.Upstreams[upstreamIndex]
	if up.Label != "" {
		log = log.With(zap.String("cluster", up.Label))

		var err error
		sd, err = util.StatsdWithTags(sd, []string{fmt.Sprintf("cluster:%s", up.Label)})
		if err != nil {
			return nil, err
		}
	}
	p := Proxy{
		log:    log,
		statsd: sd,
		config: config,

		upstreamConfigHost: up.UpstreamConfigHost,
		localConfigHost:    localSocketPathFromUpstream(up.UpstreamConfigHost, up.Database, up.Readonly, config.LocalSocketPrefix, config.LocalSocketSuffix),
		minPoolSize:        up.MinPoolSize,
		maxPoolSize:        up.MaxPoolSize,
		readTimeout:        up.ReadTimeout,
		writeTimeout:       up.WriteTimeout,
		database:           up.Database,
		readonly:           up.Readonly,
		idleTimeout:        up.IdleTimeout,

		quit: make(chan interface{}),
		kill: make(chan interface{}),

		listeners:    make(map[string]*listener.Listener),
		reservations: handlers.NewReservations(up.MaxSubscriptions, up.MaxBlockers, sd),
	}
	i, d := util.StatsdBackgroundGauge(sd, "proxy.listeners", []string{})
	p.statsdCounters.listenerInc = StatsdBackgroundGaugeCallback(i)
	p.statsdCounters.listenerDec = StatsdBackgroundGaugeCallback(d)
	return &p, nil
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
	}
	p.log.Info("Created Listener", zap.String("localHost", p.localConfigHost), zap.String("upstreamHost", p.upstreamConfigHost))
	defer func() {
		p.listenerWg.Wait()
	}()

	p.listenerLock.Lock()
	p.listeners[p.upstreamConfigHost] = ls
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
				var hostPorts []string
				for _, line := range lines {
					lt := strings.IndexByte(line, ' ')
					rt := strings.IndexByte(line, '@')
					if lt > 0 && rt > 0 {
						hostPorts = append(hostPorts, line[lt+1:rt])
					}
				}
				p.ensureNewListenersRemoveOld(hostPorts)
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

// ensureNewListenersRemoveOld() creates new Listeners for nodes that didn't exist before
// It also cleans up existing listeners for which no nodes exist anymore (with the exception of local config host)
func (p *Proxy) ensureNewListenersRemoveOld(newNodes []string) {
	nodesToRemove, nodesToAdd := compareNewNodesWithExisting(newNodes, p.getListenerKeys())
	for _, node := range nodesToRemove {
		// we should never remove the upstreamConfigHost unless we're adding it right back
		if node == p.upstreamConfigHost {
			continue
		}
		p.removeListener(node)
	}

	// finally add the new ones
	for _, node := range nodesToAdd {
		p.ensureListenerForUpstream(node, "Topology Refresh")
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
	_, ok := p.listeners[upstream]
	if !ok {
		local := localSocketPathFromUpstream(upstream, p.database, p.readonly, p.config.LocalSocketPrefix, p.config.LocalSocketSuffix)
		p.log.Info("did not find listener, creating new one", zap.String("upstream", upstream), zap.String("local", local), zap.String("command", originalCmd))
		ls, err := p.createListener(local, upstream)
		if err != nil {
			p.log.Error("unable to create listener", zap.Error(err))
		}
		p.listeners[upstream] = ls
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
	if p.idleTimeout > 0 {
		opts = append(opts, pool.WithIdleTimeout(func(time.Duration) time.Duration { return p.idleTimeout }))
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
	p.statsdCounters.listenerInc("", []string{fmt.Sprintf("upstream:%s", upstream)})
	return listener, nil
}

// Go through all listeners and health check their servers
// This should ideally by its own class with a healthcheck strategy
// Due to timeline pressures we'll just use this basic scheme
// to avoid repeated loops of Timeout on the redis client
func (p *Proxy) healthCheckConnections() {
	duration := p.config.HealthCheckCycle
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
	for i := 0; i < p.config.HealthCheckThreshold; i++ {
		time.Sleep(1 * time.Second)
		localAddress := localSocketPathFromUpstream(key, p.database, p.readonly, p.config.LocalSocketPrefix, p.config.LocalSocketSuffix)
		healthy = pingServer(p.config.Network, localAddress, p.readTimeout, p.writeTimeout, p.log)
		p.log.Debug("Finished pinging server", zap.String("server", key), zap.String("healthy", strconv.FormatBool(healthy)))
		if healthy {
			break
		}
	}
	if !healthy {
		p.log.Warn("Server failed to respond; Deleting the listener", zap.String("server", key))
		p.removeListener(key)
		if key == p.upstreamConfigHost {
			// add the upstream config host back; we always need to have that minimally
			// but hopefully this time, the connection is re-established to the right IP
			p.ensureListenerForUpstream(key, "")
		}
	}
}

// Safely delete a listener from the map
func (p *Proxy) removeListener(key string) {
	p.listenerLock.Lock()
	ls, ok := p.listeners[key]
	if ok {
		ls.Shutdown()
		delete(p.listeners, key)
		p.statsdCounters.listenerDec("", []string{fmt.Sprintf("upstream:%s", key)})
	}
	p.listenerLock.Unlock()
}

// Get a range of keys in the listeners collection
func (p *Proxy) getListenerKeys() []string {
	p.listenerLock.Lock()
	defer p.listenerLock.Unlock()
	var keys []string
	for key := range p.listeners {
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

// given an array of new nodes and an array of existing nodes it determines
// which nodes don't exist and needs to be added; also which nodes are no longer a part of the cluster
func compareNewNodesWithExisting(newNodes []string, existingNodes []string) (nodesToRemove, nodesToAdd []string) {
	var newNodesMap = make(map[string]bool)
	for _, node := range newNodes {
		newNodesMap[node] = true
	}
	// compare with existing nodes
	for _, node := range existingNodes {
		if _, ok := newNodesMap[node]; ok {
			// duplicate: remove
			delete(newNodesMap, node)
			continue
		}
		nodesToRemove = append(nodesToRemove, node)
	}
	for node := range newNodesMap {
		nodesToAdd = append(nodesToAdd, node)
	}
	return
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
	defer func() {
		_ = conn.Close()
	}()
	err = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err != nil {
		if logger != nil {
			logger.Error("failed to set write deadline", zap.Error(err))
		}
		return false
	}
	_, err = conn.Write([]byte(ping))
	if err != nil {
		if logger != nil {
			logger.Error("failed to write PING", zap.Error(err))
		}
		return false
	}
	err = conn.SetReadDeadline(time.Now().Add(readTimeout))
	if err != nil {
		if logger != nil {
			logger.Error("failed to set read deadline", zap.Error(err))
		}
		return false
	}
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
