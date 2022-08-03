package config

import (
	"errors"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"time"
)

type Options struct {
	Pretty       bool
	Statsd       string
	Level        zapcore.Level
	ConfigUrl    string
	PollInterval time.Duration
}

func ParseFlags() (*Options, error) {
	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS] uri1 [uri2] ...\n", os.Args[0])
		flag.PrintDefaults()
	}

	var stats, loglevel, configUrl string
	var pretty bool
	var pollInterval uint
	flag.StringVar(&stats, "statsd", defaultStatsdAddress, "Statsd address")
	flag.BoolVar(&pretty, "pretty", false, "Pretty print logging")
	flag.StringVar(&loglevel, "loglevel", "info", "One of: debug, info, warn, error, dpanic, panic, fatal")
	flag.StringVar(&configUrl, "config", "", "location of config file. can be local path or remote url")
	flag.UintVar(&pollInterval, "pollinterval", 30, "poll interval for config in seconds")

	// todo remove these flags in a follow up, after all envs have updated to the new url-param style of timeout config
	var obsoleteArg string
	flag.StringVar(&obsoleteArg, "readtimeout", "unused", "unused. for backwards compatibility only")
	flag.StringVar(&obsoleteArg, "writetimeout", "unused", "unused. for backwards compatibility only")

	flag.Parse()

	level := zap.InfoLevel
	if loglevel != "" {
		err := level.Set(loglevel)
		if err != nil {
			return nil, fmt.Errorf("invalid loglevel: %s", loglevel)
		}
	}

	if configUrl == "" {
		return nil, errors.New("-url must be present")
	}

	return &Options{
		Pretty:       pretty,
		Statsd:       stats,
		Level:        level,
		ConfigUrl:    configUrl,
		PollInterval: time.Duration(pollInterval) * time.Second,
	}, nil
}
