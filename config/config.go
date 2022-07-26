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

var validNetworks = []string{"tcp", "tcp4", "tcp6", "unix", "unixpacket"}

type Config struct {
	Pretty    bool
	Statsd    string
	Level     zapcore.Level
	Listeners []*Listener
	Upstreams []*Upstream
}

type Listener struct {
	Name              string
	Network           string
	LocalSocketPrefix string
	LocalSocketSuffix string
	LogLevel          zapcore.Level
	Target            string
	MaxSubscriptions  int
	MaxBlockers       int
	Unlink            bool
	Mirroring         *RequestMirrorPolicy
}

type Upstream struct {
	Name         string
	Address      string
	Database     int
	MaxPoolSize  int
	MinPoolSize  int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Readonly     bool
}

type RequestMirrorPolicy struct {
	Upstream string
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

	var stats, loglevel string
	var pretty bool
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

	var upstreams []*Upstream
	var listeners []*Listener
	for _, arg := range flag.Args() {
		all := strings.FieldsFunc(arg, func(r rune) bool {
			return r == '|' || r == '\n'
		})
		for _, v := range all {
			u, err := url.Parse(v)
			if err != nil {
				return nil, err
			}

			if u.Scheme == "redis" {
				us, err := parseUpstream(u)
				if err != nil {
					return nil, err
				}
				upstreams = append(upstreams, us)
			} else {
				ls, err := parseListener(u)
				if err != nil {
					return nil, err
				}
				listeners = append(listeners, ls)
			}
		}
	}

	if len(upstreams) == 0 {
		return nil, errors.New("missing list of upstream hosts")
	}

	if len(listeners) == 0 {
		return nil, errors.New("missing list of listeners")
	}

	addrMap := make(map[string]bool)
	for _, c := range upstreams {
		key := c.Address + "/" + strconv.Itoa(c.Database)
		if c.Readonly {
			key += "-readonly"
		}
		_, ok := addrMap[key]
		if ok {
			return nil, fmt.Errorf("duplicate entry for address: %v", c.Address)
		}
		addrMap[key] = true
	}

	return &Config{
		Upstreams: upstreams,
		Listeners: listeners,
		Pretty:    pretty,
		Statsd:    stats,
		Level:     level,
	}, nil
}

func parseUpstream(u *url.URL) (*Upstream, error) {
	var err error

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

	upstream := Upstream{
		Address:      u.Host,
		Name:         getStringParam(params, "label", ""),
		MaxPoolSize:  getIntParam(params, "maxpoolsize", 10),
		MinPoolSize:  getIntParam(params, "minpoolsize", 1),
		Database:     db,
		ReadTimeout:  rt,
		WriteTimeout: wt,
		Readonly:     getBoolParam(params, "readonly"),
	}

	return &upstream, nil
}

func parseListener(u *url.URL) (*Listener, error) {
	var err error

	if !validNetwork(u.Scheme) {
		return nil, fmt.Errorf("invalid network: %s", u.Scheme)
	}

	params, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}

	level := zap.InfoLevel
	loglevel := getStringParam(params, "loglevel", "")
	if loglevel != "" {
		err := level.Set(loglevel)
		if err != nil {
			return nil, fmt.Errorf("invalid loglevel: %s", loglevel)
		}
	}

	listener := Listener{
		Network:           u.Scheme,
		Name:              getStringParam(params, "label", ""),
		LocalSocketPrefix: getStringParam(params, "localsocketprefix", "/var/tmp/redisbetween-"),
		LocalSocketSuffix: getStringParam(params, "localsocketsuffix", ".sock"),
		Target:            getStringParam(params, "target", ""),
		MaxSubscriptions:  getIntParam(params, "maxsubscriptions", 1),
		MaxBlockers:       getIntParam(params, "maxblockers", 1),
		Unlink:            getBoolParam(params, "unlink"),
		LogLevel:          level,
	}

	if h := getStringParam(params, "mirrorTarget", ""); len(h) > 0 {
		listener.Mirroring = &RequestMirrorPolicy{
			Upstream: h,
		}
	}

	return &listener, nil
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
