package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/d2army/redisbetween/config"
	"github.com/d2army/redisbetween/proxy"
)

func main() {
	c := config.ParseFlags()
	log := newLogger(c.Level, c.Pretty)
	err := run(log, c)
	if err != nil {
		log.Panic("error", zap.Error(err))
	}
}

func newLogger(level zapcore.Level, pretty bool) *zap.Logger {
	var c zap.Config
	if pretty {
		c = zap.NewDevelopmentConfig()
		c.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		c = zap.NewProductionConfig()
	}

	c.EncoderConfig.MessageKey = "message"
	c.Level.SetLevel(level)

	log, err := c.Build(zap.AddStacktrace(zap.FatalLevel))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	return log
}

func run(log *zap.Logger, cfg *config.Config) error {
	proxies, err := proxies(cfg, log)
	if err != nil {
		log.Fatal("Startup error", zap.Error(err))
	}

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
	}()

	for _, p := range proxies {
		p := p
		wg.Add(1)
		go func() {
			err := p.Run()
			if err != nil {
				log.Error("Error", zap.Error(err))
			}
			wg.Done()
		}()
	}

	shutdown := func() {
		for _, p := range proxies {
			p.Shutdown()
		}
	}
	kill := func() {
		for _, p := range proxies {
			p.Kill()
		}
	}
	shutdownOnSignal(log, shutdown, kill)

	log.Info("Running")

	return nil
}

func proxies(c *config.Config, log *zap.Logger) (proxies []*proxy.Proxy, err error) {
	s, err := statsd.New(c.Statsd, statsd.WithNamespace("redisbetween"))
	if err != nil {
		return nil, err
	}
	for index := range c.Upstreams {
		p, err := proxy.NewProxy(log, s, c, index)

		if err != nil {
			return nil, err
		}
		proxies = append(proxies, p)
	}
	return
}

func shutdownOnSignal(log *zap.Logger, shutdownFunc func(), killFunc func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		shutdownAttempted := false
		for sig := range c {
			log.Info("Signal", zap.String("signal", sig.String()))

			if !shutdownAttempted {
				log.Info("Shutting down")
				go shutdownFunc()
				shutdownAttempted = true

				if sig == os.Interrupt {
					time.AfterFunc(1*time.Second, func() {
						fmt.Println("Ctrl-C again to kill incoming connections")
					})
				}
			} else if sig == os.Interrupt {
				log.Warn("Terminating")
				_ = log.Sync() // #nosec
				killFunc()
			}
		}
	}()
}
