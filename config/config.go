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
	Unlink            bool
	MinPoolSize       uint64
	MaxPoolSize       uint64
	Pretty            bool
	Statsd            string
	Level             zapcore.Level
	Upstreams         []Upstream
}

type Upstream struct {
	UpstreamConfigHost string
	Label              string
	MaxPoolSize        int
	MinPoolSize        int
	Database           int
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	CachePrefixes      []string
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
		fmt.Printf("Usage: %s [OPTIONS] uri1 [uri2] ...\n", os.Args[0])
		flag.PrintDefaults()
	}

	var network, localSocketPrefix, localSocketSuffix, stats, loglevel string
	var pretty, unlink bool
	flag.StringVar(&network, "network", "unix", "One of: tcp, tcp4, tcp6, unix or unixpacket")
	flag.StringVar(&localSocketPrefix, "localsocketprefix", "/var/tmp/redisbetween-", "Prefix to use for unix socket filenames")
	flag.StringVar(&localSocketSuffix, "localsocketsuffix", ".sock", "Suffix to use for unix socket filenames")
	flag.BoolVar(&unlink, "unlink", false, "Unlink existing unix sockets before listening")
	flag.StringVar(&stats, "statsd", defaultStatsdAddress, "Statsd address")
	flag.BoolVar(&pretty, "pretty", false, "Pretty print logging")
	flag.StringVar(&loglevel, "loglevel", "info", "One of: debug, info, warn, error, dpanic, panic, fatal")

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

	if !validNetwork(network) {
		return nil, fmt.Errorf("invalid network: %s", network)
	}

	var upstreams []Upstream
	for _, arg := range flag.Args() {
		all := strings.FieldsFunc(arg, func(r rune) bool {
			return r == '|' || r == '\n'
		})
		for _, v := range all {
			u, err := url.Parse(v)
			if err != nil {
				return nil, err
			}

			db := -1
			if len(u.Path) > 1 {
				db, err = strconv.Atoi(u.Path[1:])
				if err != nil {
					return nil, errors.New("failed to parse redis db number from path")
				}
			}

			params, err := url.ParseQuery(u.RawQuery)
			if err != nil {
				return nil, err
			}

			rt, err := time.ParseDuration(getStringParam(params, "readtimeout", "5s"))
			if err != nil {
				return nil, err
			}
			wt, err := time.ParseDuration(getStringParam(params, "writetimeout", "5s"))
			if err != nil {
				return nil, err
			}

			us := Upstream{
				UpstreamConfigHost: u.Host,
				Label:              getStringParam(params, "label", ""),
				MaxPoolSize:        getIntParam(params, "maxpoolsize", 10),
				MinPoolSize:        getIntParam(params, "minpoolsize", 1),
				Database:           db,
				ReadTimeout:        rt,
				WriteTimeout:       wt,
				CachePrefixes:      getStringsParam(params, "cache_prefixes", nil),
			}

			upstreams = append(upstreams, us)
		}
	}

	if len(upstreams) == 0 {
		return nil, errors.New("missing list of upstream hosts")
	}

	addrMap := make(map[string]bool)
	for _, c := range upstreams {
		key := c.UpstreamConfigHost + "/" + strconv.Itoa(c.Database)
		_, ok := addrMap[key]
		if ok {
			return nil, fmt.Errorf("duplicate entry for address: %v", c.UpstreamConfigHost)
		}
		addrMap[key] = true
	}

	return &Config{
		Upstreams:         upstreams,
		Network:           network,
		LocalSocketPrefix: localSocketPrefix,
		LocalSocketSuffix: localSocketSuffix,
		Unlink:            unlink,
		Pretty:            pretty,
		Statsd:            stats,
		Level:             level,
	}, nil
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

func getStringsParam(v url.Values, key string, def []string) []string {
	cl, ok := v[key]
	if !ok {
		return def
	}
	return strings.Split(cl[0], ",")
}
