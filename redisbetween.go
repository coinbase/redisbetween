package main

import (
	"context"
	"fmt"
	"github.com/coinbase/redisbetween/utils"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/redisbetween/config"
	"github.com/coinbase/redisbetween/proxy"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const disconnectTimeout = 10 * time.Second

func main() {
	c := config.ParseFlags()
	log := newLogger(c.Level, c.Pretty)
	s := newStatsd(c, log)

	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxStatsdKey, s), utils.CtxLogKey, log)

	err := run(ctx, c)
	if err != nil {
		log.Panic("error", zap.Error(err))
	}
}

func newStatsd(c *config.Config, log *zap.Logger) *statsd.Client {
	s, err := statsd.New(c.Statsd, statsd.WithNamespace("redisbetween"))
	if err != nil {
		log.Panic("Failed to initialize statsd", zap.Error(err))
	}
	return s
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

func run(ctx context.Context, cfg *config.Config) error {
	log := ctx.Value(utils.CtxLogKey).(*zap.Logger)
	sd := ctx.Value(utils.CtxStatsdKey).(statsd.ClientInterface)

	upstreamManager := proxy.NewUpstreamManager()

	for _, u := range cfg.Upstreams {
		err := upstreamManager.Add(ctx, u)
		if err != nil {
			log.Error("Failed to initialized upstreams")
			return err
		}
	}

	proxies, err := proxies(ctx, cfg, upstreamManager)
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
			c := context.WithValue(context.WithValue(context.Background(), utils.CtxLogKey, log), utils.CtxStatsdKey, sd)
			err := p.Run(c)
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
		ctx, cancel := context.WithTimeout(ctx, disconnectTimeout)
		_ = upstreamManager.Shutdown(ctx)
		cancel()
	}
	kill := func() {
		for _, p := range proxies {
			p.Kill()
		}

		ctx, cancel := context.WithTimeout(ctx, disconnectTimeout)
		_ = upstreamManager.Shutdown(ctx)
		cancel()
	}
	shutdownOnSignal(log, shutdown, kill)

	log.Info("Running")

	return nil
}

func proxies(ctx context.Context, c *config.Config, lookup proxy.UpstreamManager) (proxies []*proxy.Proxy, err error) {
	for _, u := range c.Listeners {
		p, err := proxy.NewProxy(ctx, c, u, lookup)

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
