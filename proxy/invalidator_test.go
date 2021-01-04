package proxy

import (
	"github.cbhq.net/engineering/redisbetween/redis"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestInvalidator_SubscribeCommand(t *testing.T) {
	i := Invalidator{clientID: 42}
	expected := redis.NewCommand("CLIENT", "TRACKING", "on", "REDIRECT", "42", "BCAST", "PREFIX", "a:", "PREFIX", "b")
	assert.Equal(t, expected, i.SubscribeCommand([]string{"a:", "b"}))
}

func TestNewInvalidator(t *testing.T) {
	server, client := net.Pipe()
	go assertConnect(t, server, nil)
	i, err := NewInvalidator(
		"hello",
		InvalidatorConnFunc(func(network, addr string) (net.Conn, error) {
			return client, nil
		}),
	)
	assert.NoError(t, err)
	err = i.Shutdown()
	assert.NoError(t, err)
}

//func TestInvalidator_Run(t *testing.T) {
//	c := populatedCache(t, map[string]string{"a:1": "val", "b:2": "hi"})
//	server, client := net.Pipe()
//	go assertConnect(t, server, func() {
//		msg := redis.NewArray([]*redis.Message{
//			redis.NewBulkBytes([]byte("message")),
//			redis.NewBulkBytes([]byte("__redis__:invalidate")),
//			redis.NewArray([]*redis.Message{
//				redis.NewBulkBytes([]byte("a:1")),
//			}),
//		})
//		err := redis.Encode(server, msg)
//		assert.NoError(t, err)
//	})
//
//	i, err := NewInvalidator(
//		"hello",
//		InvalidatorConnFunc(func(network, addr string) (net.Conn, error) {
//			return client, nil
//		}),
//	)
//	assert.NoError(t, err)
//	go i.Run(c)
//	time.Sleep(1 * time.Millisecond)
//	err = i.Shutdown()
//	assert.NoError(t, err)
//
//	m, err := c.Get([]byte("a:1"))
//	assert.Error(t, err)
//	assert.Nil(t, m)
//
//	m, err = c.Get([]byte("b:2"))
//	assert.NoError(t, err)
//	assert.Equal(t, redis.NewString([]byte("hi")), m)
//}

func TestInvalidator_Run_Reconnect(t *testing.T) {
	c := populatedCache(t, map[string]string{"a:1": "val", "b:2": "hi"})
	server, client := net.Pipe()

	go assertConnect(t, server, func() {
		_, err := server.Write([]byte("!BOGUS 🎠 🎸\r\n"))
		assert.NoError(t, err)
		//wg.Done()
	})

	i, err := NewInvalidator(
		"hello",
		//InvalidatorLogger(zaptest.NewLogger(t)),
		InvalidatorConnFunc(func(network, addr string) (net.Conn, error) {
			return client, nil
		}),
	)
	assert.NoError(t, err)
	go i.Run(c)
	//time.Sleep(1 * time.Millisecond)
	//err = i.Shutdown()
	//assert.NoError(t, err)
}

func assertConnect(t *testing.T, server net.Conn, cb func()) {
	t.Helper()
	m, err := redis.Decode(server)
	assert.NoError(t, err)
	assert.Equal(t, redis.NewCommand("CLIENT", "ID"), m)

	err = redis.Encode(server, redis.NewInt([]byte(redis.Itoa(42))))
	assert.NoError(t, err)

	m, err = redis.Decode(server)
	assert.NoError(t, err)
	assert.Equal(t, redis.NewCommand("SUBSCRIBE", "__redis__:invalidate"), m)

	err = redis.Encode(server, redis.NewString([]byte("OK")))
	assert.NoError(t, err)

	if cb != nil {
		cb()
	}
}

func populatedCache(t *testing.T, d map[string]string) *Cache {
	t.Helper()
	c := NewCache()
	for k, v := range d {
		key := [][]byte{[]byte(k)}
		val := redis.NewString([]byte(v))
		c.Set(key, val)
	}
	return c
}
