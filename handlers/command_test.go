package handlers

import (
	"testing"

	"github.com/d2army/redisbetween/redis"
	"github.com/stretchr/testify/assert"
)

func TestValidateCommands(t *testing.T) {
	c := connection{}
	wm := []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("GET")),
			redis.NewBulkBytes([]byte("hi")),
		}),
	}
	incomingCmds, err := c.validateCommands(wm)
	assert.NoError(t, err)
	assert.Equal(t, []string{"GET"}, incomingCmds)
}

func TestValidateCommandsUnsupported(t *testing.T) {
	c := connection{}
	wm := []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("BRPOP")),
			redis.NewBulkBytes([]byte("hi")),
		}),
	}
	_, err := c.validateCommands(wm)
	assert.Error(t, err)
}

func TestValidateCommandsClosedTransaction(t *testing.T) {
	c := connection{}
	wm := []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("MULTI")),
		}),
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("GET")),
			redis.NewBulkBytes([]byte("hi")),
		}),
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("EXEC")),
		}),
	}
	incomingCmds, err := c.validateCommands(wm)
	assert.NoError(t, err)
	assert.Equal(t, []string{"MULTI", "GET", "EXEC"}, incomingCmds)
}

func TestValidateCommandsOpenTransaction(t *testing.T) {
	c := connection{}
	wm := []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("WATCH")),
			redis.NewBulkBytes([]byte("hi")),
		}),
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("GET")),
			redis.NewBulkBytes([]byte("hi")),
		}),
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("MULTI")),
		}),
	}
	incomingCmds, err := c.validateCommands(wm)
	assert.Error(t, err)
	assert.Equal(t, []string{"WATCH", "GET", "MULTI"}, incomingCmds)
}

func TestConnectionClose(t *testing.T) {
	conns, _, _, _, _ := createConnectionMocks(t, 1)
	c := conns[0]

	err := c.Close()
	assert.NoError(t, err)
	assert.True(t, c.isClosed)
	assert.True(t, c.conn.(*netConnMock).closed)
}
