package handlers

import (
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/d2army/redisbetween/redis"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestIsBlockingCommand(t *testing.T) {
	t.Parallel()

	t.Run("valid command", func(t *testing.T) {
		t.Parallel()
		valid := isBlockingCommand([]string{"BRPOPLPUSH source dest 1"})
		assert.True(t, valid)
	})

	t.Run("invalid command", func(t *testing.T) {
		t.Parallel()
		invalid := isBlockingCommand([]string{"GET"})
		assert.False(t, invalid)
	})

	t.Run("multiple valid commands", func(t *testing.T) {
		t.Parallel()
		invalid := isBlockingCommand([]string{"BRPOPLPUSH source dest 1", "BRPOPLPUSH source dest 1"})
		assert.False(t, invalid)
	})

	t.Run("multiple invalid commands", func(t *testing.T) {
		t.Parallel()
		invalid := isBlockingCommand([]string{"GET", "BRPOPLPUSH source dest 1"})
		assert.False(t, invalid)
	})
}

func TestParseCommand(t *testing.T) {
	t.Parallel()

	wm := cmd("BRPOPLPUSH S D 10")
	start := time.Now()
	bc, err := parseCommand(nil, wm)

	assert.NoError(t, err)
	assert.Equal(t, wm, bc.command)
	assert.Equal(t, 10*time.Second, bc.timeout)
	assert.True(t, bc.start.After(start))
}

func TestNewBlocker(t *testing.T) {
	t.Parallel()

	conns, _, _, _, _ := createConnectionMocks(t, 1)
	c := conns[0]

	t.Run("creates blocker", func(t *testing.T) {
		c.reservations = NewReservations(1, 1, nil)
		prefix := "s1"

		b, err := newBlocker(c, prefix, zap.NewNop())

		assert.NoError(t, err)
		assert.Equal(t, prefix, b.source)
		assert.Equal(t, 1, len(b.parent.list))
		assert.Equal(t, b, b.parent.get("b", prefix))
	})

	t.Run("fails on max reservations", func(t *testing.T) {
		c.reservations = NewReservations(1, 0, nil)
		prefix := "s2"

		b, err := newBlocker(c, prefix, zap.NewNop())

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max blockers")
		assert.Nil(t, b)
		assert.Equal(t, 0, len(c.reservations.list))
		assert.Nil(t, c.reservations.get("b", prefix))
	})

	t.Run("fails on existing reservation", func(t *testing.T) {
		c.reservations = NewReservations(1, 2, nil)
		prefix := "s3"

		b1, err := newBlocker(c, prefix, zap.NewNop())
		assert.NoError(t, err)
		assert.Equal(t, prefix, b1.source)

		b2, err := newBlocker(c, prefix, zap.NewNop())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
		assert.Nil(t, b2)
	})
}

func TestEnqueue(t *testing.T) {
	t.Parallel()

	conns, _, _, _, _ := createConnectionMocks(t, 1)
	c := conns[0]

	t.Run("enqueues the command", func(t *testing.T) {
		c.reservations = NewReservations(1, 1, nil)

		b, err := newBlocker(c, "enqueue-test", zap.NewNop())
		assert.NoError(t, err)

		bc, err := parseCommand(c, cmd("BRPOPLPUSH enqueue-test dest 1"))
		assert.NoError(t, err)

		assert.False(t, b.working)
		err = b.enqueue(c, bc)
		assert.NoError(t, err)
		assert.True(t, b.working)
	})

	t.Run("fails if reservation closed", func(t *testing.T) {
		c.reservations = NewReservations(1, 1, nil)

		b, err := newBlocker(c, "enqueue-closed", zap.NewNop())
		assert.NoError(t, err)

		bc, err := parseCommand(c, cmd("BRPOPLPUSH enqueue-closed dest 1"))
		assert.NoError(t, err)

		assert.False(t, b.working)
		b.closed = true
		err = b.enqueue(c, bc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "reservation is closed")
		assert.False(t, b.working)
	})

	t.Run("fails if upstream disconnected", func(t *testing.T) {
		c.reservations = NewReservations(1, 1, nil)

		b, err := newBlocker(c, "enqueue-disconnected", zap.NewNop())
		assert.NoError(t, err)

		bc, err := parseCommand(c, cmd("BRPOPLPUSH enqueue-disconnected dest 1"))
		assert.NoError(t, err)

		assert.False(t, b.working)
		b.upstream.Close()
		err = b.enqueue(c, bc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "upstream is disconnected")
		assert.False(t, b.working)
	})
}

func TestDequeue(t *testing.T) {
	t.Parallel()

	conns, _, _, m, _ := createConnectionMocks(t, 2)
	c := conns[0]

	b, _ := newBlocker(c, "s", zap.NewNop())

	bc1, _ := parseCommand(conns[0], cmd("BRPOPLPUSH s d 1"))
	bc2, _ := parseCommand(conns[1], cmd("BRPOPLPUSH s d 1"))

	b.queue = append(b.queue, bc1)
	b.queue = append(b.queue, bc2)

	b.dequeue()

	assert.True(t, m.messagesExist(createLocalAddressForMock(0)))
	assert.True(t, m.messagesExist(createLocalAddressForMock(1)))
	assert.True(t, b.closed)
}

func TestAdjust(t *testing.T) {
	t.Parallel()

	var command []*redis.Message
	var parts []*redis.Message

	parts = append(parts, redis.NewBulkBytes([]byte("BRPOPLPUSH")))
	parts = append(parts, redis.NewBulkBytes([]byte("sourcelist")))
	parts = append(parts, redis.NewBulkBytes([]byte("destlist")))
	parts = append(parts, redis.NewBulkBytes([]byte("1")))

	command = append(command, redis.NewArray(parts))

	bc := blockingCommand{
		command: command,
	}

	t.Run("standard adjustment", func(t *testing.T) {
		bc.command[0].Array[3].Value = []byte(fmt.Sprintf("%f", 1.0))
		bc.timeout = time.Second
		bc.start = time.Now().Add(time.Duration(-500) * time.Millisecond)
		done := bc.adjust()

		assert.False(t, done)
		assert.Less(t, bc.timeout.Seconds(), time.Second.Seconds())
		assert.Greater(t, bc.timeout.Seconds(), time.Duration(0).Seconds())

		cmdTimeout := bc.command[0].Array[3].Value
		assert.Equal(t, []byte(fmt.Sprintf("%f", bc.timeout.Seconds())), cmdTimeout)
	})

	t.Run("timeout exceeded", func(t *testing.T) {
		bc.timeout = time.Second
		bc.start = time.Now().Add(time.Duration(-2) * time.Second)
		done := bc.adjust()

		assert.True(t, done)
		assert.Equal(t, time.Second, bc.timeout)
	})

	t.Run("empty timeout", func(t *testing.T) {
		bc.timeout = time.Duration(0) * time.Second
		done := bc.adjust()

		assert.False(t, done)
		assert.Equal(t, time.Duration(0)*time.Second, bc.timeout)
	})
}

func TestUpstreamDisconnect(t *testing.T) {
	t.Parallel()

	// Setup
	conns, rs, _, msgr, logObs := createConnectionMocks(t, 4)
	msgr.readErr = io.EOF

	// Send blocking messages
	err := handleBlocker(conns[0], getDefaultBlockerMessage())
	assert.NoError(t, err)
	err = handleBlocker(conns[1], getDefaultBlockerMessage())
	assert.NoError(t, err)
	err = handleBlocker(conns[2], getDefaultBlockerMessage())
	assert.NoError(t, err)
	err = handleBlocker(conns[3], getDefaultBlockerMessage())
	assert.NoError(t, err)
	bl := rs.get("b", "default").(*blocker)

	for !isClosed(bl) {
		time.Sleep(10 * time.Millisecond)
	}

	logs := logObs.All()

	// Test that the upstream disconnect code was hit
	assert.Equal(t, 1, len(msgr.messagesWritten)) // Only 1 should be written if the upstream is disconnected
	assert.True(t, logContainsAtLeastOne(logs, "Upstream disconnected"))

	// Ensure all local connections were closed
	for i := range conns {
		assert.True(t, conns[i].isClosed)
	}
}

func TestClusteredRedis(t *testing.T) {
	t.Parallel()

	// Setup
	conns, rs, _, msgr, _ := createConnectionMocks(t, 1)
	msgr.response = []*redis.Message{
		redis.NewError([]byte("CROSSSLOT ...")),
	}

	// Send blocking messages and wait for the blocker to close
	err := handleBlocker(conns[0], getDefaultBlockerMessage())
	assert.NoError(t, err)
	bl := rs.get("b", "default").(*blocker)
	for !isClosed(bl) {
		time.Sleep(10 * time.Millisecond)
	}

	// Ensure the error message was written to the local
	written := msgr.peekMessages(createLocalAddressForMock(0))
	expectedErr := fmt.Errorf("blocking commands aren't supported for Redis cluster")
	expectedMsg := []*redis.Message{redis.NewError([]byte(fmt.Sprintf("redisbetween: %v", expectedErr.Error())))}
	assert.True(t, redisArraysEqual(written[1], expectedMsg))
}

func getDefaultBlockerMessage() []*redis.Message {
	return cmd("BRPOPLPUSH default queue1 1")
}

func logContainsAtLeastOne(logs []observer.LoggedEntry, value string) bool {
	for i := range logs {
		log := logs[i]
		if log.Message == value {
			return true
		}
	}

	return false
}

func isClosed(b *blocker) bool {
	b.Lock()
	defer b.Unlock()

	return b.closed
}

func cmd(cmd string) []*redis.Message {
	parts := strings.Split(cmd, " ")
	messages := []*redis.Message{}

	for _, part := range parts {
		message := redis.NewBulkBytes([]byte(part))
		messages = append(messages, message)
	}

	wm := []*redis.Message{
		redis.NewArray(messages),
	}

	return wm
}
