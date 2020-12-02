package redis

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"go.uber.org/zap"
	"strings"
	"sync"
)

type Redis interface {
	Close()
	Do(m *Message) ([]byte, error)
}

type baseClient struct {
	log    *zap.Logger
	statsd *statsd.Client
	mu     sync.RWMutex
}

type single struct {
	baseClient
	pool *radix.Pool
}

type cluster struct {
	baseClient
	cluster *radix.Cluster
}

func Connect(log *zap.Logger, sd *statsd.Client, network string, host string, isCluster bool, poolSize int) (Redis, error) {
	var c Redis
	bc := baseClient{
		log:    log,
		statsd: sd,
	}

	// TODO new plan:
	// - connect to the primary and call CLUSTER SLOTS to discover all the nodes to start
	// - create a new local socket and upstream connection pool for each one
	// - when a message comes in, intercept only:
	// --- CLUSTER SLOTS should return the list of local sockets instead of the remote IPs
	// --- MOVED should look to see if it references a new IP that hasn't been seen before, make a new pool + sockets if so
	// --- when there is a connection error to an upstream (unreachable), remove that connection from the pool. when the
	//     pool gets empty then remove the whole pool

	if !isCluster {
		p, err := radix.NewPool(network, host, poolSize, radix.PoolPipelineWindow(0, 0))
		if err != nil {
			return nil, err
		}
		c = &single{
			baseClient: bc,
			pool:       p,
		}
	} else {
		cl, err := radix.NewCluster([]string{host}, radix.ClusterPoolFunc(func(network, addr string) (radix.Client, error) {
			// override the ClusterPoolFunc because by default radix enables secondary reads via READONLY, which we
			// do not support yet.
			return radix.NewPool(network, addr, poolSize, radix.PoolConnFunc(radix.DefaultConnFunc))
		}))
		if err != nil {
			return nil, err
		}
		c = &cluster{
			baseClient: bc,
			cluster:    cl,
		}
	}
	return c, nil
}

func (s *single) Do(m *Message) ([]byte, error) {
	command, args, err := s.prepareMessage(m)
	if err != nil {
		return nil, err
	}
	rcv := resp2.RawMessage{} // NOTE: when using RawMessage as the receiver, the error returned by Do is always nil
	_ = s.pool.Do(radix.Cmd(&rcv, command, args[1:]...))
	return rcv, nil
}

func (s *single) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pool == nil { // already closed
		return
	}
	s.log.Info("Disconnect")
	err := s.pool.Close()
	s.pool = nil
	if err != nil {
		s.log.Info("Error disconnecting", zap.Error(err))
	}
}

func (c *cluster) Do(m *Message) ([]byte, error) {
	command, args, err := c.prepareMessage(m)
	if err != nil {
		return nil, err
	}
	if AggregateCommand(command) {
		topo := c.cluster.Topo()
		primaries := topo.Primaries()
		if len(primaries) == 0 {
			return nil, errors.New("can't find a primary")
		}
		wg := sync.WaitGroup{}
		wg.Add(len(primaries))
		results := make([][]byte, len(primaries))
		for i, n := range primaries {
			go func(i int, n radix.ClusterNode) {
				pool, err := c.cluster.Client(n.Addr)
				if err != nil {
					wg.Done()
					return
				}
				rcv := resp2.RawMessage{}
				_ = pool.Do(radix.Cmd(&rcv, command, args[1:]...))
				results[i] = rcv
				wg.Done()
				return
			}(i, n)
		}
		wg.Wait()

		if !allEqual(results) {
			return nil, errors.New("got differing responses from aggregate command")
		}
		return results[0], nil
	} else {
		rcv := resp2.RawMessage{}
		_ = c.cluster.Do(radix.Cmd(&rcv, command, args[1:]...))
		return rcv, nil
	}
}

func (c *cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cluster == nil { // already closed
		return
	}
	c.log.Info("Disconnect")
	err := c.cluster.Close()
	c.cluster = nil
	if err != nil {
		c.log.Info("Error disconnecting", zap.Error(err))
	}
}

func (s *baseClient) prepareMessage(m *Message) (string, []string, error) {
	var command string
	args, err := EncodeToArgs(m)
	s.log.Debug("request", zap.Strings("command", args))
	if len(args) == 0 || err != nil {
		return "", nil, fmt.Errorf("failed to encode message")
	}

	command = strings.ToUpper(args[0])
	if !KnownCommand(command) {
		s.log.Debug("unknown command", zap.Strings("command", args))
	}

	// note that this includes the CLUSTER command
	if UnsupportedCommand(command) {
		s.log.Debug("unsupported command", zap.Strings("command", args))
		return "", nil, fmt.Errorf("unsupported command %v", command)
	}
	return command, args, nil
}

func allEqual(a [][]byte) bool {
	for i := 1; i < len(a); i++ {
		if !bytes.Equal(a[i], a[0]) {
			return false
		}
	}
	return true
}
