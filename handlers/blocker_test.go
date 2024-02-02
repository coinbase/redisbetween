package handlers_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/d2army/redisbetween/proxy"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

const (
	source      = "source-queue"
	destination = "dest-queue"
)

func TestBasicBRPOPLPUSH(t *testing.T) {
	ctx := context.Background()

	// Setup
	id := randomInteger()
	sd := proxy.SetupProxyAdvancedConfig(t, "7006", -1, 2, id, false)
	client := setupStandaloneClient(t, id)

	defer sd()
	defer cleanupClient(t, client)

	sVal := "abc"
	timeout := time.Second

	t.Run("handles an empty queue", func(t *testing.T) {
		client.Del(ctx, source, destination)

		v, err := client.BRPopLPush(ctx, source, destination, timeout).Result()
		assert.Error(t, err)
		assert.Empty(t, v)
	})

	t.Run("moves the expected value", func(t *testing.T) {
		client.Del(ctx, source, destination)

		len, err := client.LLen(ctx, source).Result()
		assert.NoError(t, err)
		assert.Zero(t, len)

		// Seed the right queue
		client.LPush(ctx, source, sVal)

		// Send a BRPOPLPUSH command
		v, err := client.BRPopLPush(ctx, source, destination, timeout).Result()
		assert.NoError(t, err)
		assert.Equal(t, v, sVal)

		// Verify that it got there
		dVal, err := client.LPop(ctx, destination).Result()
		assert.NoError(t, err)

		// Ensure we received it
		assert.NoError(t, err)
		assert.Equal(t, sVal, dVal)
	})
}

func TestMultipleBlockingClients(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	var clients []*redis.Client

	ctx := context.Background()

	// Setup
	id := randomInteger()
	sd := proxy.SetupProxyAdvancedConfig(t, "7006", -1, 2, id, false)

	defer sd()

	baseClient := setupStandaloneClient(t, id)
	defer cleanupClient(t, baseClient)

	n := 5
	for i := 0; i < n; i++ {
		client := setupStandaloneClient(t, id)
		clients = append(clients, client)
		defer cleanupClient(t, client)
	}

	t.Run("handles all commands with zero timeout", func(t *testing.T) {
		var wg sync.WaitGroup
		timeout := 0 * time.Second
		baseClient.Del(ctx, source, destination)

		for index, client := range clients {
			wg.Add(2)

			// Initiate a BRPOPLPUSH command on each client.
			go func(index int, client *redis.Client) {
				defer wg.Done()
				v, err := client.BRPopLPush(ctx, source, destination, timeout).Result()
				assert.NoError(t, err)
				assert.Contains(t, v, "created by client")
			}(index, client)

			// Push some data into the source list, but with a little jitter.
			go func(index int) {
				defer wg.Done()
				time.Sleep(time.Duration(rand.Intn(10)+1) * time.Millisecond)
				baseClient.LPush(ctx, source, fmt.Sprintf("created by client %d", index))
			}(index)
		}

		wg.Wait()
	})

	t.Run("timesout after the first command with nonzero timeout", func(t *testing.T) {
		var wg sync.WaitGroup
		timeout := 1 * time.Second
		baseClient.Del(ctx, source, destination)

		for index, client := range clients {
			wg.Add(2)

			// Initiate a BRPOPLPUSH command on each client.
			go func(index int, client *redis.Client) {
				defer wg.Done()
				v, err := client.BRPopLPush(ctx, source, destination, timeout).Result()

				if index == 0 {
					assert.NoError(t, err)
					assert.Contains(t, v, "created by client")
				} else {
					assert.Error(t, err)
					assert.EqualError(t, err, redis.Nil.Error())
					assert.Empty(t, v)
				}
			}(index, client)

			go func(index int) {
				defer wg.Done()
				time.Sleep(999 * time.Millisecond)
				baseClient.LPush(ctx, source, fmt.Sprintf("created by client %d", index))
			}(index)
		}

		wg.Wait()
	})
}
