package proxy

import (
	"context"
	"fmt"
	"github.com/coinbase/redisbetween/config"
	redis2 "github.com/coinbase/redisbetween/redis"
	"github.com/coinbase/redisbetween/utils"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"strconv"
	"sync"
	"testing"
	"time"
)

// assumes a redis cluster running with 6 nodes on 127.0.0.1 ports 7000-7005, and
// a standalone redis on port 7006. see docker-compose.yml

func TestProxy(t *testing.T) {
	sd, _ := SetupProxy(t, "7006", -1, nil)

	client := SetupStandaloneClient(t, "/var/tmp/redisbetween-1-"+utils.RedisHost()+"-7006.sock")
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

func TestRequestMirroring(t *testing.T) {
	key := uuid.New().String()
	value := uuid.New().String()

	mirror := redis.NewClient(&redis.Options{Addr: utils.RedisHost() + ":7007"})
	defer func() {
		_ = mirror.Close()
	}()

	res := mirror.Do(context.Background(), "DEL", key)
	assert.NoError(t, res.Err())

	mirroring := config.RequestMirrorPolicy{Upstream: utils.RedisHost() + ":7007"}
	shutdown, _ := SetupProxy(t, "7006", -1, &mirroring)
	defer func() {
		shutdown()
	}()

	client := SetupStandaloneClient(t, "/var/tmp/redisbetween-1-"+utils.RedisHost()+"-7006.sock")
	defer func() {
		_ = client.Close()
	}()
	res = client.Do(context.Background(), "DEL", key)
	assert.NoError(t, res.Err())

	res = client.Do(context.Background(), "SET", key, value)
	assert.NoError(t, res.Err())

	time.Sleep(1 * time.Second)
	res = mirror.Do(context.Background(), "GET", key)
	assert.NoError(t, res.Err())
	assert.Equal(t, fmt.Sprintf("GET %s: %s", key, value), res.String())
}

func TestMirroringDynamicBehaviour(t *testing.T) {
	key := uuid.New().String()
	value := uuid.New().String()

	mirror := redis.NewClient(&redis.Options{Addr: utils.RedisHost() + ":7007"})
	defer func() {
		_ = mirror.Close()
	}()

	res := mirror.Do(context.Background(), "SET", key, "world")
	assert.NoError(t, res.Err())

	shutdown, proxy := SetupProxy(t, "7006", -1, &config.RequestMirrorPolicy{Upstream: utils.RedisHost() + ":7007"})
	defer func() {
		shutdown()
	}()

	err := proxy.Update(&config.Listener{
		Name:      proxy.listenerConfig.Name,
		Target:    proxy.listenerConfig.Target,
		Mirroring: nil,
	})
	assert.NoError(t, err)

	client := SetupStandaloneClient(t, "/var/tmp/redisbetween-1-"+utils.RedisHost()+"-7006.sock")
	defer func() {
		_ = client.Close()
	}()
	res = client.Do(context.Background(), "DEL", key)
	assert.NoError(t, res.Err())

	res = client.Do(context.Background(), "SET", key, value)
	assert.NoError(t, res.Err())

	<-time.After(1 * time.Second)
	res = mirror.Do(context.Background(), "GET", key)
	assert.NoError(t, res.Err())
	assert.NotEqual(t, fmt.Sprintf("GET %s: %s", key, value), res.String())

	err = proxy.Update(&config.Listener{
		Name:      proxy.listenerConfig.Name,
		Target:    proxy.listenerConfig.Target,
		Mirroring: &config.RequestMirrorPolicy{Upstream: utils.RedisHost() + ":7007"},
	})
	assert.NoError(t, err)

	<-time.After(1 * time.Second)

	newValue := uuid.New().String()
	res = client.Do(context.Background(), "SET", key, newValue)
	assert.NoError(t, res.Err())
	<-time.After(1 * time.Second)
	res = mirror.Do(context.Background(), "GET", key)
	assert.NoError(t, res.Err())
	assert.Equal(t, fmt.Sprintf("GET %s: %s", key, newValue), res.String())
}

type command struct {
	cmd  string
	args []string
	res  string
}

func TestIntegrationCommands(t *testing.T) {
	shutdownProxy, _ := SetupProxy(t, "7000", -1, nil)
	clusterClient := SetupClusterClient(t, "/var/tmp/redisbetween-1-"+utils.RedisHost()+"-7000.sock", false, 1)
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
	shutdownProxy, _ := SetupProxy(t, "7006", 3, nil)
	client := SetupStandaloneClient(t, "/var/tmp/redisbetween-1-"+utils.RedisHost()+"-7006-3.sock")
	var i int
	var wg sync.WaitGroup
	for {
		go func(index int, t *testing.T) {
			var j int
			ind := strconv.Itoa(index)
			commands := []command{{cmd: "get", args: []string{string(redis2.PipelineSignalStartKey)}, res: "get 🔜: redis: nil"}}
			for {
				j++
				if j == 20 {
					break
				}
				s := ind + strconv.Itoa(j)
				commands = append(commands, command{cmd: "set", args: []string{s, "hi"}, res: "set " + s + " hi: OK"})
				commands = append(commands, command{cmd: "get", args: []string{s}, res: "get " + s + ": hi"})
			}
			commands = append(commands, command{cmd: "get", args: []string{string(redis2.PipelineSignalEndKey)}, res: "get 🔚: redis: nil"})
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
	shutdown, _ := SetupProxy(t, "7006", 3, nil)
	client := SetupStandaloneClient(t, "/var/tmp/redisbetween-1-"+utils.RedisHost()+"-7006-3.sock")
	res := client.Do(context.Background(), "CLIENT", "LIST")
	assert.NoError(t, res.Err())
	assert.Contains(t, res.String(), "db=3")
	shutdown()
}

func TestReadonlyCommand(t *testing.T) {
	shutdown, _ := SetupProxyAdvancedConfig(t, utils.RedisHost()+":7000", -1, 1, 1, true, nil)
	client := SetupClusterClient(t, "/var/tmp/redisbetween-1-"+utils.RedisHost()+"-7000-ro.sock", true, 1)
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

func TestUpdateConfig(t *testing.T) {
	cfg := &config.Listener{
		Name:              uuid.New().String(),
		Network:           uuid.New().String(),
		LocalSocketPrefix: uuid.New().String(),
		LocalSocketSuffix: uuid.New().String(),
		LogLevel:          zap.InfoLevel,
		Target:            uuid.New().String(),
		MaxSubscriptions:  9,
		MaxBlockers:       99,
		Unlink:            false,
		Mirroring: &config.RequestMirrorPolicy{
			Upstream: uuid.New().String(),
		},
	}
	conf := *cfg
	proxy := &Proxy{listenerConfig: &conf}

	newTarget := uuid.New().String()
	err := proxy.Update(&config.Listener{
		Name:              uuid.New().String(),
		Network:           uuid.New().String(),
		LocalSocketPrefix: uuid.New().String(),
		LocalSocketSuffix: uuid.New().String(),
		LogLevel:          zap.ErrorLevel,
		Target:            newTarget,
		MaxSubscriptions:  99,
		MaxBlockers:       9,
		Unlink:            true,
		Mirroring:         nil,
	})
	assert.NoError(t, err)
	assert.Equal(t, cfg.Name, proxy.listenerConfig.Name)
	assert.Equal(t, cfg.Network, proxy.listenerConfig.Network)
	assert.Equal(t, cfg.LocalSocketPrefix, proxy.listenerConfig.LocalSocketPrefix)
	assert.Equal(t, cfg.LocalSocketSuffix, proxy.listenerConfig.LocalSocketSuffix)
	assert.NotEqual(t, cfg.Target, proxy.listenerConfig.Target)
	assert.Equal(t, newTarget, proxy.listenerConfig.Target)
	assert.Equal(t, cfg.MaxSubscriptions, proxy.listenerConfig.MaxSubscriptions)
	assert.Equal(t, cfg.MaxBlockers, proxy.listenerConfig.MaxBlockers)
	assert.Equal(t, cfg.Unlink, proxy.listenerConfig.Unlink)
	assert.NotEqual(t, cfg.Mirroring, proxy.listenerConfig.Mirroring)
	assert.Nil(t, proxy.listenerConfig.Mirroring)
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
