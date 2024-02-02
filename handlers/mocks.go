package handlers

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/memcachedbetween/pool"
	"github.com/d2army/redisbetween/redis"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

/* ConnectionWrapper mock */
type connWrapperMock struct {
	netConnMock netConnMock
	closed      bool
	returned    bool
}

func (m *connWrapperMock) Conn() net.Conn {
	return &m.netConnMock
}

func (m *connWrapperMock) Close() error {
	m.closed = true
	return nil
}

func (m *connWrapperMock) Return() error {
	m.returned = true
	return nil
}

func (m *connWrapperMock) Alive() bool {
	return !m.closed
}

func (m *connWrapperMock) ID() uint64 {
	return 1
}

func (m *connWrapperMock) Address() pool.Address {
	return "localhost"
}

func (m *connWrapperMock) LocalAddress() pool.Address {
	return "localhost"
}

/* ServerWrapper mock */
type serverWrapperMock struct {
	connWrapperMock *connWrapperMock
}

func (m *serverWrapperMock) Connect() error {
	return nil
}

func (m *serverWrapperMock) Disconnect(ctx context.Context) error {
	return nil
}

func (m *serverWrapperMock) Connection(ctx context.Context) (pool.ConnectionWrapper, error) {
	return m.connWrapperMock, nil
}

/* net.Conn mock */
type netConnMock struct {
	closed bool
}

func (m *netConnMock) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m *netConnMock) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (m *netConnMock) Close() error {
	m.closed = true
	return nil
}

func (m *netConnMock) LocalAddr() net.Addr {
	return nil
}

func (m *netConnMock) RemoteAddr() net.Addr {
	return nil
}

func (m *netConnMock) SetDeadline(t time.Time) error {
	return nil
}

func (m *netConnMock) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *netConnMock) SetWriteDeadline(t time.Time) error {
	return nil
}

/* messenger.Messenger mock */
type messengerMock struct {
	lock            sync.Mutex
	messagesWritten map[string][][]*redis.Message
	readErr         error
	response        []*redis.Message
}

func (m *messengerMock) peekMessages(key string) [][]*redis.Message {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.messagesWritten[key]
}

func (m *messengerMock) messagesExist(key string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	_, ok := m.messagesWritten[key]
	return ok
}

func (m *messengerMock) Write(ctx context.Context, log *zap.Logger, wm []*redis.Message, nc net.Conn, address string, id uint64, writeTimeout time.Duration, wrapPipeline bool, close func() error) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.messagesWritten[address]; !ok {
		m.messagesWritten[address] = make([][]*redis.Message, 1)
	}

	m.messagesWritten[address] = append(m.messagesWritten[address], wm)
	return nil
}

func (m *messengerMock) Read(ctx context.Context, log *zap.Logger, nc net.Conn, address string, id uint64, readTimeout time.Duration, readMin int, checkPipelineSignals bool, close func() error) ([]*redis.Message, error) {
	return m.response, m.readErr
}

/* Helpers */
func createConnectionMocks(t *testing.T, numMocks uint64) ([]*connection, *Reservations, *serverWrapperMock, *messengerMock, *observer.ObservedLogs) {
	t.Helper()

	sd, err := statsd.New("localhost:8125")
	assert.NoError(t, err)

	sm := &serverWrapperMock{
		connWrapperMock: &connWrapperMock{
			netConnMock: netConnMock{},
		},
	}

	rs := &Reservations{
		list:    make(map[string]reservation),
		maxSub:  1,
		maxBlk:  1,
		monitor: newReservationsMonitor(sd),
	}

	msgr := &messengerMock{
		messagesWritten: make(map[string][][]*redis.Message),
		lock:            sync.Mutex{},
		response: []*redis.Message{
			redis.NewArray([]*redis.Message{
				redis.NewBulkBytes([]byte("PONG")),
			}),
		},
	}

	conns := make([]*connection, 0)

	observedCore, observedLogs := observer.New(zap.InfoLevel)
	observedLogger := zap.New(observedCore)

	for i := uint64(0); i < numMocks; i++ {
		conn := &connection{
			log:          observedLogger,
			statsd:       sd,
			ctx:          context.Background(),
			conn:         &netConnMock{},
			address:      createLocalAddressForMock(i),
			id:           i,
			server:       sm,
			kill:         make(chan interface{}),
			interceptor:  func(incomingCmds []string, m []*redis.Message) {},
			reservations: rs,
			messenger:    msgr,
		}

		conns = append(conns, conn)
	}

	return conns, rs, sm, msgr, observedLogs
}

func createLocalAddressForMock(id uint64) string {
	return fmt.Sprintf("testaddress%v", id)
}
