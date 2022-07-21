package redis

import (
	"context"
	"github.com/coinbase/redisbetween/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRedisCallToPerformRoundTripOperation(t *testing.T) {
	client, _ := NewClient(context.TODO(), &Options{
		addr: utils.RedisHost() + ":7006",
	})

	commands := []*Message{
		NewArray([]*Message{
			NewBulkBytes([]byte("SET")),
			NewBulkBytes([]byte("hi")),
			NewBulkBytes([]byte("1")),
		}),
	}

	result, err := client.Call(context.TODO(), commands)
	assert.Nil(t, err, err)
	assert.Equal(t, len(result), 1)
	assert.Equal(t, []byte("OK"), result[0].Value)
}

func TestRedisCallToPerformRoundTripOperationWithMultipleCommands(t *testing.T) {
	client, _ := NewClient(context.TODO(), &Options{
		addr: utils.RedisHost() + ":7006",
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

	result, err := client.Call(context.TODO(), commands)
	assert.Nil(t, err, err)
	assert.Equal(t, len(result), 2)
	assert.Equal(t, []byte("OK"), result[0].Value)
	assert.Equal(t, []byte("1"), result[1].Value)
}
