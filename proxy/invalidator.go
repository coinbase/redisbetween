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
	err := redis.Encode(i.conn, redis.NewCommand("CLIENT", "ID"))
	if err != nil {
		return err
	}

	m, err := redis.Decode(i.conn)
	if err != nil || !m.IsInt() {
		return err
	}
	id, _ := redis.Btoi64(m.Value)
	i.clientID = id

	err = redis.Encode(i.conn, redis.NewCommand("SUBSCRIBE", "__redis__:invalidate"))
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
	parts := []string{"CLIENT", "TRACKING", "on", "REDIRECT", redis.Itoa(i.clientID), "BCAST"}
	for _, p := range prefixes {
		parts = append(parts, "PREFIX", p)
	}
	return redis.NewCommand(parts...)
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
