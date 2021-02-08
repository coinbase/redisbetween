package handlers

import (
	"context"
	"github.com/coinbase/redisbetween/redis"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	"net"
	"sync"
	"testing"
	"time"
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
			redis.NewBulkBytes([]byte("SUBSCRIBE")),
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

func TestReadWireMessagesPipeline(t *testing.T) {
	commands := []string{
		"*2\r\n$3\r\nGET\r\n$4\r\n🔜\r\n",
		"*3\r\n$3\r\nSET\r\n$2\r\nhi\r\n$1\r\n1\r\n",
		"*2\r\n$3\r\nGET\r\n$2\r\nhi\r\n",
		"*2\r\n$3\r\nGET\r\n$4\r\n🔚\r\n",
	}
	expected := []string{
		"*3 \\r\\n $3 \\r\\n SET \\r\\n $2 \\r\\n hi \\r\\n $1 \\r\\n 1 \\r\\n ",
		"*2 \\r\\n $3 \\r\\n GET \\r\\n $2 \\r\\n hi \\r\\n ",
	}
	testReadWireMessagesHelper(t, 1, true, commands, expected)
}

func TestReadWireMessagesSingle(t *testing.T) {
	commands := []string{"*3\r\n$3\r\nSET\r\n$2\r\nhi\r\n$1\r\n1\r\n"}
	expected := []string{"*3 \\r\\n $3 \\r\\n SET \\r\\n $2 \\r\\n hi \\r\\n $1 \\r\\n 1 \\r\\n "}
	testReadWireMessagesHelper(t, 1, true, commands, expected)
}

func TestReadWireMessagesSingleNoPipeline(t *testing.T) {
	commands := []string{"*3\r\n$3\r\nSET\r\n$2\r\nhi\r\n$1\r\n1\r\n"}
	expected := []string{"*3 \\r\\n $3 \\r\\n SET \\r\\n $2 \\r\\n hi \\r\\n $1 \\r\\n 1 \\r\\n "}
	testReadWireMessagesHelper(t, 1, false, commands, expected)
}

func TestReadWireMessagesMultiNoPipeline(t *testing.T) {
	commands := []string{
		"*3\r\n$3\r\nSET\r\n$2\r\nhi\r\n$1\r\n1\r\n",
		"*3\r\n$3\r\nSET\r\n$2\r\nhi\r\n$1\r\n2\r\n",
		"*3\r\n$3\r\nSET\r\n$2\r\nhi\r\n$1\r\n3\r\n",
	}
	expected := []string{
		"*3 \\r\\n $3 \\r\\n SET \\r\\n $2 \\r\\n hi \\r\\n $1 \\r\\n 1 \\r\\n ",
		"*3 \\r\\n $3 \\r\\n SET \\r\\n $2 \\r\\n hi \\r\\n $1 \\r\\n 2 \\r\\n ",
		"*3 \\r\\n $3 \\r\\n SET \\r\\n $2 \\r\\n hi \\r\\n $1 \\r\\n 3 \\r\\n ",
	}
	testReadWireMessagesHelper(t, 3, false, commands, expected)
}

func testReadWireMessagesHelper(t *testing.T, readMin int, checkPipelineSignals bool, commands []string, expected []string) {
	t.Helper()
	wg := sync.WaitGroup{}
	wg.Add(1)

	reader, writer := net.Pipe()
	_ = writer.SetDeadline(time.Now().Add(1 * time.Second))
	go func(t *testing.T) {
		wm, err := ReadWireMessages(
			context.Background(),
			zaptest.NewLogger(t),
			reader,
			"",
			0,
			1*time.Second,
			readMin,
			checkPipelineSignals,
			reader.Close,
		)
		actuals := make([]string, 0)
		for _, m := range wm {
			actuals = append(actuals, m.String())
		}
		assert.NoError(t, err)
		assert.Equal(t, expected, actuals)
		wg.Done()
	}(t)

	for _, c := range commands {
		_, _ = writer.Write([]byte(c))
	}

	wg.Wait()
	_ = writer.Close()
}

func TestWriteWireMessagesNoPipelineWrapping(t *testing.T) {
	commands := []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("SET")),
			redis.NewBulkBytes([]byte("hi")),
			redis.NewBulkBytes([]byte("1")),
		}),
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("GET")),
			redis.NewBulkBytes([]byte("hi")),
		}),
	}
	expected := []string{
		"*3 \\r\\n $3 \\r\\n SET \\r\\n $2 \\r\\n hi \\r\\n $1 \\r\\n 1 \\r\\n ",
		"*2 \\r\\n $3 \\r\\n GET \\r\\n $2 \\r\\n hi \\r\\n ",
	}
	testWriteWireMessagesHelper(t, false, commands, expected)
}

func TestWriteWireMessagesWithPipelineWrapping(t *testing.T) {
	commands := []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("SET")),
			redis.NewBulkBytes([]byte("hi")),
			redis.NewBulkBytes([]byte("1")),
		}),
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("GET")),
			redis.NewBulkBytes([]byte("hi")),
		}),
	}
	expected := []string{
		"$-1 \\r\\n ",
		"*3 \\r\\n $3 \\r\\n SET \\r\\n $2 \\r\\n hi \\r\\n $1 \\r\\n 1 \\r\\n ",
		"*2 \\r\\n $3 \\r\\n GET \\r\\n $2 \\r\\n hi \\r\\n ",
		"$-1 \\r\\n ",
	}
	testWriteWireMessagesHelper(t, true, commands, expected)
}

func testWriteWireMessagesHelper(t *testing.T, wrapPipeline bool, wm []*redis.Message, expected []string) {
	t.Helper()
	wg := sync.WaitGroup{}
	wg.Add(1)
	l := len(wm)
	if wrapPipeline {
		l = l + 2
	}

	reader, writer := net.Pipe()
	_ = reader.SetDeadline(time.Now().Add(1 * time.Second))
	go func(l int, t *testing.T) {
		actuals := make([]string, l)
		for i := 0; i < l; i++ {
			m, err := redis.Decode(reader)
			assert.NoError(t, err)
			actuals[i] = m.String()
		}

		assert.Equal(t, expected, actuals)
		_ = reader.Close()
		wg.Done()
	}(l, t)

	err := WriteWireMessages(context.Background(), zaptest.NewLogger(t), wm, writer, "", 0, 1*time.Second, wrapPipeline, writer.Close)
	assert.NoError(t, err)
	wg.Wait()
}
