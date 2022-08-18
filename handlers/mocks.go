package handlers

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/memcachedbetween/pool"
	"github.com/coinbase/redisbetween/config"
	"github.com/coinbase/redisbetween/redis"
	"github.com/stretchr/testify/assert"
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

func (m *serverWrapperMock) Disconnect(_ context.Context) error {
	return nil
}

func (m *serverWrapperMock) Connection(_ context.Context) (pool.ConnectionWrapper, error) {
	return m.connWrapperMock, nil
}

/* net.Conn mock */
type netConnMock struct {
	closed bool
}

func (m *netConnMock) Read(_ []byte) (n int, err error) {
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

func (m *netConnMock) SetDeadline(_ time.Time) error {
	return nil
}

func (m *netConnMock) SetReadDeadline(_ time.Time) error {
	return nil
}

func (m *netConnMock) SetWriteDeadline(_ time.Time) error {
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

type upstreamLookupMock struct {
	cfg    *config.Upstream
	client redis.ClientInterface
}

func (u *upstreamLookupMock) ConfigByName(name string) (*config.Upstream, bool) {
	return u.cfg, true
}

func (u *upstreamLookupMock) LookupByName(name string) (redis.ClientInterface, bool) {
	return u.client, true
}

type redisClientMock struct {
	sm *serverWrapperMock
}

func (r *redisClientMock) Address() string {
	return ""
}

func (r *redisClientMock) Call(_ context.Context, msg []*redis.Message) ([]*redis.Message, error) {
	return msg, nil
}

func (r *redisClientMock) CheckoutConnection(_ context.Context) (conn pool.ConnectionWrapper, err error) {
	return r.sm.connWrapperMock, nil
}

func (r *redisClientMock) Close(_ context.Context) error {
	return nil
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
			log:            observedLogger,
			statsd:         sd,
			ctx:            context.Background(),
			conn:           &netConnMock{},
			address:        createLocalAddressForMock(i),
			id:             i,
			kill:           make(chan interface{}),
			interceptor:    func(incomingCmds []string, m []*redis.Message) {},
			reservations:   rs,
			messenger:      msgr,
			upstreamLookup: &upstreamLookupMock{cfg: &config.Upstream{}, client: &redisClientMock{sm: sm}},
			listenerConfig: &config.Listener{},
		}

		conns = append(conns, conn)
	}

	return conns, rs, sm, msgr, observedLogs
}

func createLocalAddressForMock(id uint64) string {
	return fmt.Sprintf("testaddress%v", id)
}
