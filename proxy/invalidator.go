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
	_, err := i.conn.Write([]byte("*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n"))
	if err != nil {
		return err
	}

	m, err := redis.Decode(i.conn)
	if err != nil || !m.IsInt() {
		return err
	}
	id, _ := redis.Btoi64(m.Value)
	i.clientID = id

	_, err = i.conn.Write([]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$20\r\n__redis__:invalidate\r\n"))
	if err != nil {
		return err
	}
	_, err = redis.Decode(i.conn)
	if err != nil {
		return err
	}

	return nil
}

func (i *invalidator) run(cache *Cache) error {
	// TODO on any connection issue, destroy the cache
	for {
		m, err := redis.Decode(i.conn)
		if err != nil {
			return err
		}
		if !m.IsArray() || len(m.Array) < 3 {
			continue
		}
		key := m.Array[2].Value
		_ = cache.Del(key)
	}
}
