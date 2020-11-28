package config

import (
	"errors"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"regexp"
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
	//opts        *redis.Options
	//clusterOpts *redis.ClusterOptions
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
		p, err := proxy.NewProxy(log, s, client.label, c.network, client.host, client.address, client.poolSize)
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

	// TODO discuss: this looks for a non-standard URL param called cluster=true to control what type of client to use
	// not great and a bit ugly. better alternative?

	nonClusterAddressMap := make(map[string]string)
	clusterAddressMap := make(map[string]string)
	r := regexp.MustCompile(`[?&]cluster=true`)

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
			if _, ok := clusterAddressMap[split[0]]; ok {
				return nil, fmt.Errorf("uri already defined for host: %s", split[0])
			}
			if _, ok := nonClusterAddressMap[split[0]]; ok {
				return nil, fmt.Errorf("uri already defined for host: %s", split[0])
			}
			subbed := r.ReplaceAllString(split[1], "")
			if subbed == split[1] {
				nonClusterAddressMap[split[0]] = split[1]
			} else {
				clusterAddressMap[split[0]] = subbed
			}
		}
	}

	if len(clusterAddressMap) == 0 && len(nonClusterAddressMap) == 0 {
		return nil, errors.New("missing host:uri(s)")
	}

	fmt.Println("clusters", clusterAddressMap)
	fmt.Println("non clusters", nonClusterAddressMap)

	var clients []client
	for address, uri := range nonClusterAddressMap {
		label := address // TODO this seems wrong
		clients = append(clients, client{
			host:    uri,
			address: address,
			label:   label,
			//poolOpt:    nil, // TODO
			poolSize: 5,
		})
	}
	for address, uri := range clusterAddressMap {
		label := address // TODO this seems wrong
		//opts := &redis.ClusterOptions{
		//	Addrs: []string{uri},
		//	//NewClient:          nil, // TODO this might be a better way to refactor - create a normal client to use for both
		//	MaxRetries:      0,
		//	MinRetryBackoff: 0,
		//	MaxRetryBackoff: 0,
		//	PoolSize:        5,
		//	//ClusterSlots:       nil, // TODO custom function to re-resolve elasticache config endpoint
		//}
		clients = append(clients, client{
			host:    uri,
			address: address,
			label:   label,
			//clusterOpt: nil, // TODO
		})
	}

	return &Config{
		network: network,
		unlink:  unlink,
		clients: clients,
		statsd:  stats,
		level:   level,
	}, nil
}

func expandEnv(config string) string {
	// more restrictive version of os.ExpandEnv that only replaces exact matches of ${ENV}
	return regexp.MustCompile(`\${(\w+)}`).ReplaceAllStringFunc(config, func(s string) string {
		return os.ExpandEnv(s)
	})
}
