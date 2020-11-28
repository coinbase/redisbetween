package proxy

import (
	"fmt"
	"github.cbhq.net/engineering/redis-proxy/redis"
	"github.cbhq.net/engineering/redis-proxy/util"
	"runtime/debug"

	"net"
	"sync"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
)

const restartSleep = 1 * time.Second

type Proxy struct {
	log    *zap.Logger
	statsd *statsd.Client

	network string
	host    string
	address string
	unlink  bool
	//poolOpt *radix.PoolOpt
	poolSize int
	//clusterOpt *radix.ClusterOpt

	quit chan interface{}
	kill chan interface{}
}

func NewProxy(log *zap.Logger, sd *statsd.Client, label, network, host, address string, poolSize int) (*Proxy, error) {
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

		network:  network,
		host:     host,
		address:  address,
		poolSize: poolSize,
		//poolOpt: poolOpt,
		//clusterOpt: clusterOpt,

		quit: make(chan interface{}),
		kill: make(chan interface{}),
	}, nil
}

func (p *Proxy) Run() error {
	return p.run()
}

func (p *Proxy) Shutdown() {
	defer func() {
		_ = recover() // "close of closed channel" panic if Shutdown() was already called
	}()
	close(p.quit)
}

func (p *Proxy) Kill() {
	p.Shutdown()

	defer func() {
		_ = recover() // "close of closed channel" panic if Kill() was already called
	}()
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

	r, err := redis.Connect(p.log, p.statsd, p.network, p.host, p.poolSize)
	if err != nil {
		return err
	}
	defer r.Close()
	return p.listen(r)
}

func (p *Proxy) listen(r redis.Redis) error {
	if p.network == "unix" {
		oldUmask := syscall.Umask(0)
		defer syscall.Umask(oldUmask)
		if p.unlink {
			_ = syscall.Unlink(p.host)
		}
	}

	l, err := net.Listen(p.network, p.address)
	if err != nil {
		return err
	}
	defer func() {
		_ = l.Close()
	}()
	go func() {
		<-p.quit
		err := l.Close()
		if err != nil {
			p.log.Info("Error closing listener", zap.Error(err))
		}
	}()

	p.accept(l, r)
	return nil
}

func (p *Proxy) accept(l net.Listener, r redis.Redis) {
	var wg sync.WaitGroup
	defer func() {
		p.log.Info("Waiting for open connections")
		wg.Wait()
	}()

	opened, closed := util.StatsdBackgroundGauge(p.statsd, "open_connections", []string{})

	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-p.quit:
				return
			default:
				p.log.Error("Failed to accept incoming connection", zap.Error(err))
				continue
			}
		}

		log := p.log
		remoteAddr := c.RemoteAddr().String()
		if remoteAddr != "" {
			log = p.log.With(zap.String("remote_address", remoteAddr))
		}

		done := make(chan interface{})

		wg.Add(1)
		opened("connection_opened", []string{})
		go func() {
			log.Info("Accept")
			handleConnection(log, p.statsd, c, r, p.kill)

			_ = c.Close()
			log.Info("Close")

			close(done)
			wg.Done()
			closed("connection_closed", []string{})
		}()

		go func() {
			select {
			case <-done:
				// closed
			case <-p.kill:
				err := c.Close()
				if err == nil {
					log.Warn("Force closed connection")
				}
			}
		}()
	}
}
