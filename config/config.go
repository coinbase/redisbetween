package config

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const defaultStatsdAddress = "localhost:8125"
const defaultHealthCheckCycle = 60 * time.Second
const defaultHealthCheckThreshold = 3

var validNetworks = []string{"tcp", "tcp4", "tcp6", "unix", "unixpacket"}

type Config struct {
	Network              string
	LocalSocketPrefix    string
	LocalSocketSuffix    string
	Unlink               bool
	MinPoolSize          uint64
	MaxPoolSize          uint64
	Pretty               bool
	Statsd               string
	Level                zapcore.Level
	Upstreams            []Upstream
	HealthCheck          bool
	HealthCheckCycle     time.Duration
	HealthCheckThreshold int
	IdleTimeout          time.Duration
}

type Upstream struct {
	UpstreamConfigHost string
	Label              string
	MaxPoolSize        int
	MinPoolSize        int
	Database           int
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	Readonly           bool
	MaxSubscriptions   int
	MaxBlockers        int
	IdleTimeout        time.Duration
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
	var healthCheck bool
	var healthCheckThreshold int
	var healthCheckCycle, idleTimeout time.Duration
	flag.StringVar(&network, "network", "unix", "One of: tcp, tcp4, tcp6, unix or unixpacket")
	flag.StringVar(&localSocketPrefix, "localsocketprefix", "/var/tmp/redisbetween-", "Prefix to use for unix socket filenames")
	flag.StringVar(&localSocketSuffix, "localsocketsuffix", ".sock", "Suffix to use for unix socket filenames")
	flag.BoolVar(&unlink, "unlink", false, "Unlink existing unix sockets before listening")
	flag.StringVar(&stats, "statsd", defaultStatsdAddress, "Statsd address")
	flag.BoolVar(&pretty, "pretty", false, "Pretty print logging")
	flag.StringVar(&loglevel, "loglevel", "info", "One of: debug, info, warn, error, dpanic, panic, fatal")
	flag.BoolVar(&healthCheck, "healthcheck", false, "Start the routine to do health checks on redis servers")
	flag.DurationVar(&healthCheckCycle, "healthcheckcycle", defaultHealthCheckCycle, "Duration for the cycle during which server connections will be health-checked; Must be larger than healthcheckthreshold * 1s; default: 60s")
	flag.IntVar(&healthCheckThreshold, "healthcheckthreshold", defaultHealthCheckThreshold, "The number of concecutive failures needed to declare a server connection dead; default: 3")
	flag.DurationVar(&idleTimeout, "idletimeout", 0, "Timeout value that a connection can remain idle; After this a connection is recreated; default: 0 (no timeout)")

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

			us := Upstream{
				UpstreamConfigHost: u.Host,
				Label:              getStringParam(params, "label", ""),
				MaxPoolSize:        getIntParam(params, "maxpoolsize", 10),
				MinPoolSize:        getIntParam(params, "minpoolsize", 1),
				Database:           db,
				ReadTimeout:        getDurationParam(params, "readtimeout", 5*time.Second),
				WriteTimeout:       getDurationParam(params, "writetimeout", 5*time.Second),
				Readonly:           getBoolParam(params, "readonly"),
				MaxSubscriptions:   getIntParam(params, "maxsubscriptions", 1),
				MaxBlockers:        getIntParam(params, "maxblockers", 1),
				IdleTimeout:        getDurationParam(params, "idletimeout", idleTimeout),
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
		if c.Readonly {
			key += "-readonly"
		}
		_, ok := addrMap[key]
		if ok {
			return nil, fmt.Errorf("duplicate entry for address: %v", c.UpstreamConfigHost)
		}
		addrMap[key] = true
	}

	return &Config{
		Upstreams:            upstreams,
		Network:              network,
		LocalSocketPrefix:    localSocketPrefix,
		LocalSocketSuffix:    localSocketSuffix,
		Unlink:               unlink,
		Pretty:               pretty,
		Statsd:               stats,
		Level:                level,
		HealthCheck:          healthCheck,
		HealthCheckThreshold: healthCheckThreshold,
		HealthCheckCycle:     healthCheckCycle,
		IdleTimeout:          idleTimeout,
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

	val := expandEnv(cl[0])
	i, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return i
}

func getBoolParam(v url.Values, key string) bool {
	val := getStringParam(v, key, "false")
	return val == "true"
}

func expandEnv(config string) string {
	// more restrictive version of os.ExpandEnv that only replaces exact matches of ${ENV}
	return regexp.MustCompile(`\${(\w+)}`).ReplaceAllStringFunc(config, func(s string) string {
		return os.ExpandEnv(s)
	})
}

func getDurationParam(v url.Values, key string, def time.Duration) time.Duration {
	cl, ok := v[key]
	if !ok {
		return def
	}
	d, e := time.ParseDuration(cl[0])
	if e != nil {
		return def
	}
	return d
}
