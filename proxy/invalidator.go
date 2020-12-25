package proxy

import (
	"fmt"
	"github.cbhq.net/engineering/redisbetween/redis"
	"net"
)

type invalidator struct {
	conn     net.Conn
	clientId int64
}

func newInvalidator(upstream string) (*invalidator, error) {
	conn, err := net.Dial("tcp", upstream)
	if err != nil {
		return nil, err
	}

	i := &invalidator{conn: conn}
	err = i.getClientId()

	return i, err
}

func (i *invalidator) getClientId() error {
	_, err := i.conn.Write([]byte("*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n"))
	if err != nil {
		return err
	}

	m, err := redis.Decode(i.conn)
	if err != nil || !m.IsInt() {
		return err
	}
	id, err := redis.Btoi64(m.Value)
	i.clientId = id

	_, err = i.conn.Write([]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$20\r\n__redis__:invalidate\r\n"))
	if err != nil {
		return err
	}
	m, err = redis.Decode(i.conn)
	if err != nil {
		return err
	}

	return nil
}

func (i *invalidator) run() error {
	for {
		m, err := redis.Decode(i.conn)
		fmt.Println("===> !!! got message in invalidator, next step: invalidate local cache", m, err)
		if err != nil {
			return err
		}
	}
}
