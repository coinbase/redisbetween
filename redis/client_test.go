package redis

import (
	"context"
	"testing"

	"go.uber.org/zap"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/redisbetween/utils"
	"github.com/stretchr/testify/assert"
)

func TestRedisCallToPerformRoundTripOperation(t *testing.T) {
	sd, _ := statsd.New("localhost:8125")
	client, _ := NewClient(&Options{
		Addr: utils.RedisHost() + ":7006",
	}, zap.NewNop(), sd)

	commands := []*Message{
		NewArray([]*Message{
			NewBulkBytes([]byte("SET")),
			NewBulkBytes([]byte("hi")),
			NewBulkBytes([]byte("1")),
		}),
	}

	result, err := client.Call(context.Background(), commands)
	assert.Nil(t, err, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, []byte("OK"), result[0].Value)
}

func TestRedisCallToPerformRoundTripOperationWithMultipleCommands(t *testing.T) {
	sd, _ := statsd.New("localhost:8125")

	client, _ := NewClient(&Options{
		Addr: utils.RedisHost() + ":7006",
	}, zap.NewNop(), sd)

	commands := []*Message{
		NewArray([]*Message{
			NewBulkBytes([]byte("SET")),
			NewBulkBytes([]byte("hi")),
			NewBulkBytes([]byte("1")),
		}),
		NewArray([]*Message{
			NewBulkBytes([]byte("GET")),
			NewBulkBytes([]byte("hi")),
		}),
	}

	result, err := client.Call(context.Background(), commands)
	assert.Nil(t, err, err)
	assert.Equal(t, len(result), 2)
	assert.Equal(t, []byte("OK"), result[0].Value)
	assert.Equal(t, []byte("1"), result[1].Value)
}
