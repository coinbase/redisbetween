package proxy

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/d2army/redisbetween/messenger"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

// assumes a redis cluster running with 6 nodes on 127.0.0.1 ports 7000-7005, and
// a standalone redis on port 7006. see docker-compose.yml

func TestProxy(t *testing.T) {
	sd := SetupProxy(t, "7006", -1)

	client := SetupStandaloneClient(t, "/var/tmp/redisbetween-1-"+RedisHost()+"-7006.sock")
	res := client.Do(context.Background(), "del", "hello")
	assert.NoError(t, res.Err())
	res = client.Do(context.Background(), "set", "hello", "world")
	assert.NoError(t, res.Err())
	res = client.Do(context.Background(), "get", "hello")
	assert.NoError(t, res.Err())
	assert.Equal(t, "get hello: world", res.String())
	err := client.Close()
	assert.NoError(t, err)
	sd()
}

type command struct {
	cmd  string
	args []string
	res  string
}

func TestIntegrationCommands(t *testing.T) {
	shutdownProxy := SetupProxy(t, "7000", -1)
	clusterClient := SetupClusterClient(t, "/var/tmp/redisbetween-1-"+RedisHost()+"-7000.sock", false, 1)
	var i int
	var wg sync.WaitGroup
	for {
		go func(index int, t *testing.T) {
			var j int
			ind := strconv.Itoa(index)
			for {
				j++
				if j == 20 {
					wg.Done()
					break
				}
				s := ind + strconv.Itoa(j)
				assertResponse(t, command{cmd: "set", args: []string{s, "hi"}, res: "set " + s + " hi: OK"}, clusterClient)
				assertResponse(t, command{cmd: "get", args: []string{s}, res: "get " + s + ": hi"}, clusterClient)
			}
		}(i, t)
		wg.Add(1)
		i++
		if i == 10 {
			break
		}
	}
	wg.Wait()
	shutdownProxy()
}

func TestPipelinedCommands(t *testing.T) {
	shutdownProxy := SetupProxy(t, "7006", 3)
	client := SetupStandaloneClient(t, "/var/tmp/redisbetween-1-"+RedisHost()+"-7006-3.sock")
	var i int
	var wg sync.WaitGroup
	for {
		go func(index int, t *testing.T) {
			var j int
			ind := strconv.Itoa(index)
			commands := []command{{cmd: "get", args: []string{string(messenger.PipelineSignalStartKey)}, res: "get 🔜: redis: nil"}}
			for {
				j++
				if j == 20 {
					break
				}
				s := ind + strconv.Itoa(j)
				commands = append(commands, command{cmd: "set", args: []string{s, "hi"}, res: "set " + s + " hi: OK"})
				commands = append(commands, command{cmd: "get", args: []string{s}, res: "get " + s + ": hi"})
			}
			commands = append(commands, command{cmd: "get", args: []string{string(messenger.PipelineSignalEndKey)}, res: "get 🔚: redis: nil"})
			assertResponsePipelined(t, commands, client)
			wg.Done()
		}(i, t)
		wg.Add(1)
		i++
		if i == 10 {
			break
		}
	}
	wg.Wait()
	shutdownProxy()
}

func TestDbSelectCommand(t *testing.T) {
	shutdown := SetupProxy(t, "7006", 3)
	client := SetupStandaloneClient(t, "/var/tmp/redisbetween-1-"+RedisHost()+"-7006-3.sock")
	res := client.Do(context.Background(), "CLIENT", "LIST")
	assert.NoError(t, res.Err())
	assert.Contains(t, res.String(), "db=3")
	shutdown()
}

func TestReadonlyCommand(t *testing.T) {
	shutdown := SetupProxyAdvancedConfig(t, "7000", -1, 1, 1, true)
	client := SetupClusterClient(t, "/var/tmp/redisbetween-1-"+RedisHost()+"-7000-ro.sock", true, 1)
	res := client.Do(context.Background(), "CLIENT", "LIST")
	assert.NoError(t, res.Err())
	assert.Contains(t, res.String(), "flags=r")
	shutdown()
}

func TestLocalSocketPathFromUpstream(t *testing.T) {
	assert.Equal(t, "prefix-with.host-colon.suffix", localSocketPathFromUpstream("with.host:colon", -1, false, "prefix-", ".suffix"))
	assert.Equal(t, "prefix-withoutcolon.host.suffix", localSocketPathFromUpstream("withoutcolon.host", -1, false, "prefix-", ".suffix"))
	assert.Equal(t, "prefix-with.host-db-1.suffix", localSocketPathFromUpstream("with.host:db", 1, false, "prefix-", ".suffix"))
	assert.Equal(t, "prefix-with.host-db-ro.suffix", localSocketPathFromUpstream("with.host:db", -1, true, "prefix-", ".suffix"))
}

func TestPingServer(t *testing.T) {
	shutdown := SetupProxy(t, "7000", -1)
	network := "unix"
	address := "/var/tmp/redisbetween-1-" + RedisHost() + "-7000.sock"
	readTimeout := time.Second
	writeTimeout := time.Second
	assert.True(t, pingServer(network, address, readTimeout, writeTimeout, nil))
	shutdown()
}

func TestNewNodeComparison(t *testing.T) {
	existingNodes := [3]string{"host1:port1", "host2:port2", "host3:port3"}
	newNodes := [3]string{"host4:port4", "host2:port2", "host3:port3"}
	tobeRemoved, tobeAdded := compareNewNodesWithExisting(newNodes[:], existingNodes[:])
	assert.Equal(t, 1, len(tobeAdded))
	assert.Equal(t, 1, len(tobeRemoved))
	assert.Equal(t, "host4:port4", tobeAdded[0])
	assert.Equal(t, "host1:port1", tobeRemoved[0])
}

func assertResponse(t *testing.T, cmd command, c *redis.ClusterClient) {
	args := make([]interface{}, len(cmd.args)+1)
	args[0] = cmd.cmd
	for i, a := range cmd.args {
		args[i+1] = a
	}
	res := c.Do(context.Background(), args...)
	assert.Equal(t, cmd.res, res.String())
}

func assertResponsePipelined(t *testing.T, cmds []command, c *redis.Client) {
	p := c.Pipeline()
	actuals := make([]*redis.Cmd, len(cmds))
	expected := make([]string, len(cmds))
	for i, cmd := range cmds {
		args := make([]interface{}, len(cmd.args)+1)
		args[0] = cmd.cmd
		for i, a := range cmd.args {
			args[i+1] = a
		}
		actuals[i] = p.Do(context.Background(), args...)
		expected[i] = cmd.res
	}
	_, _ = p.Exec(context.Background())
	actualStrings := make([]string, len(actuals))
	for i, a := range actuals {
		actualStrings[i] = a.String()
	}
	assert.Equal(t, expected, actualStrings)
}
