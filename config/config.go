package config

import (
	"errors"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	url2 "net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/DataDog/datadog-go/statsd"

	"github.cbhq.net/engineering/redis-proxy/proxy"
)

const defaultStatsdAddress = "localhost:8125"

//var validNetworks = []string{"tcp", "tcp4", "tcp6", "unix", "unixpacket"}
var validNetworks = []string{"tcp", "unix"}

type Config struct {
	unlink  bool
	network string
	clients []client
	statsd  string
	level   zapcore.Level
}

type client struct {
	host    string
	address string
	label   string
	//poolOpt    *radix.PoolOpt
	//clusterOpt *radix.ClusterOpt
	poolSize int
	cluster  bool
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

func (c *Config) LogLevel() zapcore.Level {
	return c.level
}

func (c *Config) Pretty() bool {
	return true
}

func (c *Config) Proxies(log *zap.Logger) (proxies []*proxy.Proxy, err error) {
	s, err := statsd.New(c.statsd, statsd.WithNamespace("redis-proxy"))
	if err != nil {
		return nil, err
	}
	for _, client := range c.clients {
		p, err := proxy.NewProxy(log, s, client.label, c.network, client.host, client.address, client.cluster, client.poolSize)
		if err != nil {
			return nil, err
		}
		proxies = append(proxies, p)
	}
	return
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

	var unlink bool
	var network, stats, loglevel string
	flag.StringVar(&network, "network", "tcp", "One of: tcp or unix")
	flag.StringVar(&stats, "statsd", defaultStatsdAddress, "Statsd host")
	flag.BoolVar(&unlink, "unlink", false, "Unlink existing unix sockets before listening")
	flag.StringVar(&loglevel, "loglevel", "info", "One of: debug, info, warn, error, dpanic, panic, fatal")

	flag.Parse()

	network = expandEnv(network)
	stats = expandEnv(stats)
	loglevel = expandEnv(loglevel)

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

	var clients []client
	addrMap := make(map[string]bool)

	for _, arg := range flag.Args() {
		arg = expandEnv(arg)
		all := strings.FieldsFunc(arg, func(r rune) bool {
			return r == '|' || r == '\n'
		})
		for _, v := range all {
			split := strings.SplitN(v, "=", 2)
			if len(split) != 2 {
				return nil, errors.New("malformed host:uri option")
			}

			// split[0] -> address
			// split[1] -> uri
			url, err := url2.Parse(split[1])
			if err != nil {
				return nil, err
			}

			params, err := url2.ParseQuery(url.RawQuery)
			if err != nil {
				return nil, err
			}

			cl := client{
				address:  split[0],
				host:     split[1],
				label:    getStringParam(params, "label", ""),
				poolSize: getIntParam(params, "poolsize", 5),
				//poolOpt:    nil, // TODO
			}

			cl.cluster = hasParam(params, "cluster", "true")
			clients = append(clients, cl)
		}
	}

	if len(clients) == 0 {
		return nil, errors.New("missing host=uri(s)")
	}

	for _, c := range clients {
		_, ok := addrMap[c.address]
		if ok {
			return nil, fmt.Errorf("duplicate entry for address: %v", c.address)
		}
		addrMap[c.address] = true
	}

	return &Config{
		network: network,
		unlink:  unlink,
		clients: clients,
		statsd:  stats,
		level:   level,
	}, nil
}

func hasParam(v url2.Values, key, val string) bool {
	cl, ok := v[key]
	return ok && cl[0] == val
}

func getStringParam(v url2.Values, key, def string) string {
	cl, ok := v[key]
	if !ok {
		return def
	}
	return cl[0]
}

func getIntParam(v url2.Values, key string, def int) int {
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

func expandEnv(config string) string {
	// more restrictive version of os.ExpandEnv that only replaces exact matches of ${ENV}
	return regexp.MustCompile(`\${(\w+)}`).ReplaceAllStringFunc(config, func(s string) string {
		return os.ExpandEnv(s)
	})
}
