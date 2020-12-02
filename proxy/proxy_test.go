package proxy

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"testing"
	"time"
)

func TestProxy(t *testing.T) {
	sd := setupProxy(t, "7006", ":6380", false)

	client := setupClient(t, "redis://localhost:6380")
	err := client.Do(radix.Cmd(nil, "DEL", "hello"))
	assert.Nil(t, err)
	err = client.Do(radix.Cmd(nil, "SET", "hello", "world"))
	assert.Nil(t, err)
	rcv := ""
	err = client.Do(radix.Cmd(&rcv, "GET", "hello"))
	assert.Nil(t, err)
	assert.Equal(t, "world", rcv)
	err = client.Close()
	assert.Nil(t, err)
	sd()
}

type command struct {
	cmd  string
	args []string
	res  string
}

type doer interface {
	Do(action radix.Action) error
}

var integrationScript = []command{
	{"flushall", []string{}, "+OK\r\n"},
	{"set", []string{"hi", "hello"}, "+OK\r\n"},
	{"get", []string{"hi"}, "$5\r\nhello\r\n"},
	{"append", []string{"hi", " world"}, ":11\r\n"},
	{"get", []string{"hi"}, "$11\r\nhello world\r\n"},
	{"auth", []string{"secret"}, "-redis-proxy: unsupported command AUTH\r\n"},
	{"bgrewriteaof", []string{}, "+Background append only file rewriting started\r\n"},
	{"bitcount", []string{"hi"}, ":45\r\n"},
	{"set", []string{"bf", "0"}, "+OK\r\n"},
	{"bitfield", []string{"bf", "incrby", "u2", "100", "1"}, "*1\r\n:1\r\n"},
	{"bitcount", []string{"hi"}, ":45\r\n"},
	{"bitop", []string{"AND", "bo", "hi", "bf"}, "-redis-proxy: unsupported command BITOP\r\n"},
	{"bitpos", []string{"hi", "1"}, ":1\r\n"},
	{"set", []string{"incrdecr", "3"}, "+OK\r\n"},
	{"decr", []string{"incrdecr"}, ":2\r\n"},
	{"decrby", []string{"incrdecr", "2"}, ":0\r\n"},
	{"dump", []string{"hi"}, "$23\r\n\x00\vhello world\t\x00\xdb\x0e\x18u\xf2AÅ\r\n"},
	{"echo", []string{"coinbase"}, "$8\r\ncoinbase\r\n"},
	{"expire", []string{"incrdecr", "1"}, ":1\r\n"},
	{"expireat", []string{"incrdecr", "1293840000"}, ":1\r\n"},
	{"flushdb", []string{}, "+OK\r\n"},
	{"geoadd", []string{"Sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"}, ":2\r\n"},
	{"geodist", []string{"Sicily", "Palermo", "Catania"}, "$11\r\n166274.1516\r\n"},
	{"georadius", []string{"Sicily", "15", "37", "200", "km"}, "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n"},
	{"set", []string{"hi", "0"}, "+OK\r\n"},
	{"getset", []string{"hi", "1"}, "$1\r\n0\r\n"},
	{"hmset", []string{"myhash", "field1", "hello", "field2", "world"}, "+OK\r\n"},
	{"hget", []string{"myhash", "field1"}, "$5\r\nhello\r\n"},
	{"hmget", []string{"myhash", "field1", "field2"}, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"},
	{"move", []string{"myhash", "2"}, "-redis-proxy: unsupported command MOVE\r\n"},
	{"mset", []string{"hi{1}", "hello", "incrdecr{1}", "world"}, "+OK\r\n"},
}

func TestIntegrationCommands(t *testing.T) {
	sds := setupProxy(t, "7006", ":6380", false)
	sdc := setupProxy(t, "7000", ":6381", true)

	singleClient := setupClient(t, "redis://localhost:6380")
	clusterClient := setupClient(t, "redis://localhost:6381")

	for _, cmd := range integrationScript {
		assertResponse(t, cmd, singleClient)
		assertResponse(t, cmd, clusterClient)
	}

	eval := command{"eval", []string{"return {KEYS[1],KEYS[2],ARGV[1]}", "2", "hi", "incrdecr", ":)"}, "*3\r\n$2\r\nhi\r\n$8\r\nincrdecr\r\n$2\r\n:)\r\n"}
	assertResponse(t, eval, singleClient)
	eval.res = "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
	assertResponse(t, eval, clusterClient)

	mset := command{"mset", []string{"a", "1", "b", "1", "c", "1", "d", "1", "zzzbbq", "1"}, "+OK\r\n"}
	assertResponse(t, mset, singleClient)
	eval.res = "-CROSSSLOT Keys in request don't hash to the same slot\r\n"
	assertResponse(t, mset, clusterClient)

	sds()
	sdc()
}

func assertResponse(t *testing.T, cmd command, c doer) {
	rcv := resp2.RawMessage{}
	_ = c.Do(radix.Cmd(&rcv, cmd.cmd, cmd.args...))
	assert.Equal(t, cmd.res, string(rcv), cmd.cmd)
}

func setupProxy(t *testing.T, upstream_port, address string, cluster bool) func() {
	t.Helper()

	uri := "redis://localhost:" + upstream_port
	if os.Getenv("CI") == "true" {
		uri = "redis://redis:" + upstream_port
	}

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	proxy, err := NewProxy(zap.L(), sd, "test", "tcp", uri, address, cluster, 5)
	go func() {
		err := proxy.Run()
		assert.Nil(t, err)
	}()

	time.Sleep(1 * time.Second) // todo find a more elegant way to do this

	return func() {
		proxy.Shutdown()
	}
}

func setupClient(t *testing.T, address string) *radix.Pool {
	t.Helper()

	client, err := radix.NewPool("tcp", address, 5)
	assert.Nil(t, err)

	err = client.Do(radix.Cmd(nil, "PING"))
	if err != nil {
		_ = client.Close()
		// Use t.Fatalf instead of assert because we want to fail fast if the cluster is down.
		t.Fatalf("error pinging redis: %v", err)
	}

	return client
}
