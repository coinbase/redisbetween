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

func TestOneSubscription(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	chName := "TestOneSubscription"
	id := randomInteger()

	// Setup
	sd := proxy.SetupProxyAdvancedConfig(t, "7006", -1, 2, id, false)
	defer sd()
	subClient := setupStandaloneClient(t, id)
	defer cleanupClient(t, subClient)
	pubClient := setupStandaloneClient(t, id)
	defer cleanupClient(t, pubClient)

	pubsub, err := subscribe(ctx, subClient, chName)
	assert.NoError(t, err)

	// Publish the message
	err = pubClient.Publish(ctx, chName, "message1").Err()
	assert.NoError(t, err)

	// Ensure we received it
	msg, err := pubsub.ReceiveMessage(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "message1", msg.Payload)

	assertNoMessages(ctx, t, pubsub)
}

func TestMultiSubscription(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	var clients []*redis.Client
	var subscriptions []*redis.PubSub
	var lock sync.Mutex

	ctx := context.Background()
	chName := "TestMultiSubscription"
	id := randomInteger()

	// Setup
	sd := proxy.SetupProxyAdvancedConfig(t, "7006", -1, 2, id, false)
	defer sd()

	pubClient := setupStandaloneClient(t, id)
	defer cleanupClient(t, pubClient)

	n := 5
	for i := 0; i < n; i++ {
		client := setupStandaloneClient(t, id)
		clients = append(clients, client)
		defer cleanupClient(t, client)
	}

	for index, client := range clients {
		wg.Add(1)

		go func(index int, client *redis.Client) {
			defer wg.Done()
			pubsub, err := subscribe(ctx, client, chName)
			if err != nil {
				t.Errorf("could not subscribe client %d: %v", index, err)
			}

			lock.Lock()
			defer lock.Unlock()
			subscriptions = append(subscriptions, pubsub)
		}(index, client)
	}

	wg.Wait()

	// Publish the message
	err := pubClient.Publish(ctx, chName, "message1").Err()
	assert.NoError(t, err)

	for _, sub := range subscriptions {
		msg, err := sub.ReceiveMessage(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "message1", msg.Payload)
	}
}

func TestClusteredSubscribe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	chName := "TestClusteredSubscribe"
	id := randomInteger()
	address := fmt.Sprintf("/var/tmp/redisbetween-%d-"+proxy.RedisHost()+"-7000.sock", id)

	// Setup
	sd := proxy.SetupProxyAdvancedConfig(t, "7000", -1, 2, id, false)
	defer sd()
	subClient := proxy.SetupClusterClient(t, address, false, id)
	defer subClient.Close()
	pubClient := proxy.SetupClusterClient(t, address, false, id)
	defer pubClient.Close()

	pubsub := pubClient.Subscribe(ctx, chName)
	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	assert.NoError(t, err)

	// Publish the message
	err = pubClient.Publish(ctx, chName, "message1").Err()
	assert.NoError(t, err)

	// Ensure we received it
	msg, err := pubsub.ReceiveMessage(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "message1", msg.Payload)

	assertNoMessages(ctx, t, pubsub)
}

func TestSubscribeAfterPublish(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	chName := "TestSubscribeAfterPublish"
	id := randomInteger()

	sd := proxy.SetupProxyAdvancedConfig(t, "7006", -1, 2, id, false)
	defer sd()
	pubClient := setupStandaloneClient(t, id)
	defer cleanupClient(t, pubClient)

	// Publish the message
	err := pubClient.Publish(ctx, chName, "message1").Err()
	assert.NoError(t, err)

	// Subscribe
	subClient := setupStandaloneClient(t, id)
	defer cleanupClient(t, subClient)

	pubsub, err := subscribe(ctx, subClient, chName)
	assert.NoError(t, err)

	// Ensure we didn't receive the message
	assertNoMessages(ctx, t, pubsub)
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	chName := "TestUnsubscribe"
	id := randomInteger()

	// Setup
	sd := proxy.SetupProxyAdvancedConfig(t, "7006", -1, 2, id, false)
	defer sd()
	subClient := setupStandaloneClient(t, id)
	defer cleanupClient(t, subClient)
	pubClient := setupStandaloneClient(t, id)
	defer cleanupClient(t, pubClient)

	pubsub, err := subscribe(ctx, subClient, chName)
	assert.NoError(t, err)

	// Ensure we can receive messages
	err = pubClient.Publish(ctx, chName, "message1").Err()
	assert.NoError(t, err)
	msg, err := pubsub.ReceiveMessage(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "message1", msg.Payload)

	// Unsubscribe
	err = pubsub.Unsubscribe(ctx, chName)
	assert.NoError(t, err)

	// Publish another message
	err = pubClient.Publish(ctx, chName, "message1").Err()
	assert.NoError(t, err)

	// Ensure there were no more messages received
	assertNoMessages(ctx, t, pubsub)
}

func TestMaxSubscriptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	id := randomInteger()

	sd := proxy.SetupProxyAdvancedConfig(t, "7006", -1, 2, id, false)
	defer sd()
	subClient1 := setupStandaloneClient(t, id)
	defer cleanupClient(t, subClient1)
	subClient2 := setupStandaloneClient(t, id)
	defer cleanupClient(t, subClient2)

	_, err := subscribe(ctx, subClient1, "TestMaxSubscriptions1")
	assert.NoError(t, err)

	_, err = subscribe(ctx, subClient2, "TestMaxSubscriptions2")
	assert.Error(t, err)
}

func assertNoMessages(ctx context.Context, t *testing.T, pubsub *redis.PubSub) {
	t.Helper()

	_, err := pubsub.ReceiveTimeout(ctx, time.Second)
	assert.Error(t, err)
}

func cleanupClient(t *testing.T, client *redis.Client) {
	t.Helper()

	err := client.Close()
	assert.NoError(t, err)
}

func setupStandaloneClient(t *testing.T, id int) *redis.Client {
	return proxy.SetupStandaloneClient(t, fmt.Sprintf("/var/tmp/redisbetween-%d-"+proxy.RedisHost()+"-7006.sock", id))
}

func subscribe(ctx context.Context, client *redis.Client, channel string) (*redis.PubSub, error) {
	pubsub := client.Subscribe(ctx, channel)

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	return pubsub, err
}

func randomInteger() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Int()
}
