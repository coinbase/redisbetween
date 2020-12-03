package listener

import (
	"github.cbhq.net/engineering/redis-proxy/config"
	"github.cbhq.net/engineering/redis-proxy/redis"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"

	"github.com/coinbase/mongobetween/util"
)

const restartSleep = 1 * time.Second

type Listener struct {
	log    *zap.Logger
	statsd *statsd.Client
	cfg    *config.Config

	network  string
	address  string
	handler  ConnectionHandler
	shutdown ShutdownHandler

	quit   chan interface{}
	kill   chan interface{}
	events chan interface{}
}

type ConnectionHandler func(log *zap.Logger, conn net.Conn, id uint64, kill chan interface{}, events chan interface{})
type ShutdownHandler func()

func New(log *zap.Logger, sd *statsd.Client, cfg *config.Config, network, address string, handler ConnectionHandler, shutdown ShutdownHandler, events chan interface{}) (*Listener, error) {
	return &Listener{
		log:    log,
		statsd: sd,
		cfg:    cfg,

		network:  network,
		address:  address,
		handler:  handler,
		shutdown: shutdown,

		quit:   make(chan interface{}),
		kill:   make(chan interface{}),
		events: events,
	}, nil
}

func (l *Listener) Run() error {
	//defer func() {
	//	if r := recover(); r != nil {
	//		l.log.Error("Crashed", zap.String("panic", fmt.Sprintf("%v", r)), zap.String("stack", string(debug.Stack())))
	//
	//		time.Sleep(restartSleep)
	//
	//		l.log.Info("Restarting", zap.Duration("sleep", restartSleep))
	//		go func() {
	//			err := l.Run()
	//			if err != nil {
	//				l.log.Error("Error restarting", zap.Error(err))
	//			}
	//		}()
	//	} else {
	//		l.shutdown()
	//		l.log.Info("Shutting down")
	//	}
	//}()

	return l.listen()
}

func (l *Listener) Shutdown() {
	defer func() {
		_ = recover() // "close of closed channel" panic if Shutdown() was already called
	}()
	close(l.quit)
}

func (l *Listener) Kill() {
	l.Shutdown()

	defer func() {
		_ = recover() // "close of closed channel" panic if Kill() was already called
	}()
	close(l.kill)
}

func (l *Listener) listen() error {
	if strings.Contains(l.network, "unix") {
		oldUmask := syscall.Umask(0)
		defer syscall.Umask(oldUmask)
		if l.cfg.Unlink {
			_ = syscall.Unlink(l.address)
		}
	}

	li, err := net.Listen(l.network, l.address)
	if err != nil {
		return err
	}
	defer func() {
		_ = li.Close()
	}()
	go func() {
		<-l.quit
		err := li.Close()
		if err != nil {
			l.log.Info("Error closing listener", zap.Error(err))
		}
	}()

	l.accept(li)
	return nil
}

func (l *Listener) accept(li net.Listener) {
	var wg sync.WaitGroup
	defer func() {
		l.log.Info("Waiting for open connections")
		wg.Wait()
	}()

	opened, closed := util.StatsdBackgroundGauge(l.statsd, "open_connections", []string{})

	for {
		c, err := li.Accept()
		if err != nil {
			select {
			case <-l.quit:
				return
			default:
				l.log.Error("Failed to accept incoming connection", zap.Error(err))
				continue
			}
		}

		id := redis.NextConnectionID()
		log := l.log.With(zap.Uint64("local_id", id))

		done := make(chan interface{})

		wg.Add(1)
		opened("connection_opened", []string{})
		go func() {
			defer func() {
				_ = c.Close()
				log.Info("Close")

				close(done)
				wg.Done()
				closed("connection_closed", []string{})
			}()

			log.Info("Accept")
			l.handler(log, c, id, l.kill, l.events)
		}()

		go func() {
			select {
			case <-done:
				// closed
			case <-l.kill:
				err := c.Close()
				if err == nil {
					log.Warn("Force closed connection")
				}
			}
		}()
	}
}
