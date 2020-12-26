package proxy

import (
	"github.cbhq.net/engineering/redisbetween/redis"
	"net"
)

type invalidator struct {
	conn     net.Conn
	clientID int64
}

func newInvalidator(upstream string) (*invalidator, error) {
	conn, err := net.Dial("tcp", upstream)
	if err != nil {
		return nil, err
	}

	i := &invalidator{conn: conn}
	err = i.getClientID()

	return i, err
}

func (i *invalidator) getClientID() error {
	cmd := redis.NewArray([]*redis.Message{
		redis.NewBulkBytes([]byte("CLIENT")),
		redis.NewBulkBytes([]byte("ID")),
	})
	err := redis.Encode(i.conn, cmd)
	if err != nil {
		return err
	}

	m, err := redis.Decode(i.conn)
	if err != nil || !m.IsInt() {
		return err
	}
	id, _ := redis.Btoi64(m.Value)
	i.clientID = id

	cmd = redis.NewArray([]*redis.Message{
		redis.NewBulkBytes([]byte("SUBSCRIBE")),
		redis.NewBulkBytes([]byte("__redis__:invalidate")),
	})
	err = redis.Encode(i.conn, cmd)
	if err != nil {
		return err
	}
	_, err = redis.Decode(i.conn)
	if err != nil {
		return err
	}

	return nil
}

func (i *invalidator) subscribeCommand(prefixes []string) *redis.Message {
	parts := []*redis.Message{
		redis.NewBulkBytes([]byte("CLIENT")),
		redis.NewBulkBytes([]byte("TRACKING")),
		redis.NewBulkBytes([]byte("on")),
		redis.NewBulkBytes([]byte("REDIRECT")),
		redis.NewBulkBytes([]byte(redis.Itoa(i.clientID))),
		redis.NewBulkBytes([]byte("BCAST")),
	}
	for _, p := range prefixes {
		parts = append(parts, redis.NewBulkBytes([]byte("PREFIX")), redis.NewBulkBytes([]byte(p)))
	}
	return redis.NewArray(parts)
}

func (i *invalidator) run(cache *Cache) error {
	// TODO on any connection issue, destroy the cache
	// TODO do a simple keepalive loop to check for health? or do a canary invalidation every ~10 seconds?
	for {
		m, err := redis.Decode(i.conn)
		if err != nil {
			return err
		}
		if !m.IsArray() || len(m.Array) < 3 || !m.Array[2].IsArray() {
			continue
		}
		for _, mm := range m.Array[2].Array {
			_ = cache.Del(mm.Value)
		}
	}
}
