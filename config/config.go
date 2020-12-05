package config

import (
	"errors"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultStatsdAddress = "localhost:8125"

var validNetworks = []string{"tcp", "tcp4", "tcp6", "unix", "unixpacket"}

type Config struct {
	Network           string
	LocalSocketPrefix string
	LocalSocketSuffix string
	LocalPortStart    int
	Unlink            bool

	MinPoolSize  uint64
	MaxPoolSize  uint64
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	Pretty    bool
	Statsd    string
	Level     zapcore.Level
	Upstreams []Upstream
}

type Upstream struct {
	UpstreamConfigHost string
	LocalConfigHost    string
	Label              string
	MaxPoolSize        int
	MinPoolSize        int
	Cluster            bool
}

func ParseFlags() *Config {
	config, err := parseFlags()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		flag.Usage()
		os.Exit(2)
	}
	return config
}

func validNetwork(network string) bool {
	for _, n := range validNetworks {
		if n == network {
			return true
		}
	}
	return false
}

func parseFlags() (*Config, error) {
	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS] address1=uri1 [address2=uri2] ...\n", os.Args[0])
		flag.PrintDefaults()
	}

	var network, localSocketPrefix, localSocketSuffix, stats, loglevel string
	var localPortStart int
	var readTimeout, writeTimeout time.Duration
	var pretty, unlink bool
	flag.StringVar(&network, "network", "unix", "One of: tcp, tcp4, tcp6, unix or unixpacket")
	flag.StringVar(&localSocketPrefix, "localsocketprefix", "/var/tmp/redis-proxy-", "Prefix to use for unix socket filenames")
	flag.StringVar(&localSocketSuffix, "localsocketsuffix", ".sock", "Suffix to use for unix socket filenames")
	flag.IntVar(&localPortStart, "localportstart", 6381, "Port number to start from for local proxies")
	flag.BoolVar(&unlink, "unlink", false, "Unlink existing unix sockets before listening")
	flag.DurationVar(&readTimeout, "readtimeout", 1*time.Second, "Read timeout")
	flag.DurationVar(&writeTimeout, "writetimeout", 1*time.Second, "Write timeout")
	flag.StringVar(&stats, "statsd", defaultStatsdAddress, "Statsd address")
	flag.BoolVar(&pretty, "pretty", false, "Pretty print logging")
	flag.StringVar(&loglevel, "loglevel", "info", "One of: debug, info, warn, error, dpanic, panic, fatal")
	flag.Parse()

	level := zap.InfoLevel
	if loglevel != "" {
		err := level.Set(loglevel)
		if err != nil {
			return nil, fmt.Errorf("invalid loglevel: %s", loglevel)
		}
	}

	if !validNetwork(network) {
		return nil, fmt.Errorf("invalid network: %s", network)
	}

	var upstreams []Upstream
	addrMap := make(map[string]bool)

	for _, arg := range flag.Args() {
		all := strings.FieldsFunc(arg, func(r rune) bool {
			return r == '|' || r == '\n'
		})
		for _, v := range all {
			split := strings.SplitN(v, "=", 2)
			if len(split) != 2 {
				return nil, errors.New("malformed host:uri option")
			}

			// split[0] -> localHost
			// split[1] -> remoteHost
			u, err := url.Parse(split[1])
			if err != nil {
				return nil, err
			}

			params, err := url.ParseQuery(u.RawQuery)
			if err != nil {
				return nil, err
			}

			us := Upstream{
				UpstreamConfigHost: u.Host,
				LocalConfigHost:    split[0],
				Label:              getStringParam(params, "label", ""),
				MaxPoolSize:        getIntParam(params, "maxpoolsize", 10),
				MinPoolSize:        getIntParam(params, "minpoolsize", 1),
				Cluster:            getBoolParam(params, "cluster", "true"),
			}
			upstreams = append(upstreams, us)
		}
	}

	if len(upstreams) == 0 {
		return nil, errors.New("missing host=uri(s)")
	}

	for _, c := range upstreams {
		_, ok := addrMap[c.UpstreamConfigHost]
		if ok {
			return nil, fmt.Errorf("duplicate entry for address: %v", c.UpstreamConfigHost)
		}
		addrMap[c.UpstreamConfigHost] = true
	}

	return &Config{
		Upstreams: upstreams,

		Network:           network,
		LocalSocketPrefix: localSocketPrefix,
		LocalSocketSuffix: localSocketSuffix,
		LocalPortStart:    localPortStart,
		Unlink:            unlink,

		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,

		Pretty: pretty,
		Statsd: stats,
		Level:  level,
	}, nil
}

func getBoolParam(v url.Values, key, val string) bool {
	cl, ok := v[key]
	return ok && cl[0] == val
}

func getStringParam(v url.Values, key, def string) string {
	cl, ok := v[key]
	if !ok {
		return def
	}
	return cl[0]
}

func getIntParam(v url.Values, key string, def int) int {
	cl, ok := v[key]
	if !ok {
		return def
	}
	i, err := strconv.Atoi(cl[0])
	if err != nil {
		return def
	}
	return i
}
