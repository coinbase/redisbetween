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
	command{"set", []string{"hi", "hello"}, "+OK\r\n"},
	command{"get", []string{"hi"}, "$5\r\nhello\r\n"},
	command{"append", []string{"hi", " world"}, ":11\r\n"},
	command{"get", []string{"hi"}, "$11\r\nhello world\r\n"},
	command{"auth", []string{"secret"}, "-redis-proxy: unsupported command AUTH\r\n"},
	command{"bgrewriteaof", []string{}, "+Background append only file rewriting started\r\n"},
	command{"bitcount", []string{"hi"}, ":45\r\n"},
	command{"set", []string{"bf", "0"}, "+OK\r\n"},
	command{"bitfield", []string{"bf", "incrby", "u2", "100", "1"}, "*1\r\n:1\r\n"},
	command{"bitcount", []string{"hi"}, ":45\r\n"},
	command{"bitop", []string{"AND", "bo", "hi", "bf"}, "-redis-proxy: unsupported command BITOP\r\n"},
	command{"bitpos", []string{"hi", "1"}, ":1\r\n"},
	command{"set", []string{"incrdecr", "3"}, "+OK\r\n"},
	command{"decr", []string{"incrdecr"}, ":2\r\n"},
	command{"decrby", []string{"incrdecr", "2"}, ":0\r\n"},
	command{"dump", []string{"hi"}, "$23\r\n\x00\vhello world\t\x00\xdb\x0e\x18u\xf2AÅ\r\n"},
	command{"echo", []string{"coinbase"}, "$8\r\ncoinbase\r\n"},
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
