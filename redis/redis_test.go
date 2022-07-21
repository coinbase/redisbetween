package redis

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/coinbase/redisbetween/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
)

func TestRedisCallToPerformRoundTripOperation(t *testing.T) {
	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), utils.CtxStatsdKey, &statsd.NoOpClient{})
	client, _ := NewClient(ctx, &Options{
		Addr: utils.RedisHost() + ":7006",
	})

	commands := []*Message{
		NewArray([]*Message{
			NewBulkBytes([]byte("SET")),
			NewBulkBytes([]byte("hi")),
			NewBulkBytes([]byte("1")),
		}),
	}

	result, err := client.Call(ctx, commands)
	assert.Nil(t, err, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, []byte("OK"), result[0].Value)
}

func TestRedisCallToPerformRoundTripOperationWithMultipleCommands(t *testing.T) {
	ctx := context.WithValue(context.WithValue(context.Background(), utils.CtxLogKey, zap.L()), utils.CtxStatsdKey, &statsd.NoOpClient{})
	client, _ := NewClient(ctx, &Options{
		Addr: utils.RedisHost() + ":7006",
	})

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

	result, err := client.Call(ctx, commands)
	assert.Nil(t, err, err)
	assert.Equal(t, len(result), 2)
	assert.Equal(t, []byte("OK"), result[0].Value)
	assert.Equal(t, []byte("1"), result[1].Value)
}
