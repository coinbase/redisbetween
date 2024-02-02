package handlers

import (
	"testing"

	"github.com/d2army/redisbetween/redis"
	"github.com/stretchr/testify/assert"
)

const testChannelName = "chatter"

func TestIsSubscriptionMessageWithValidMsg(t *testing.T) {
	t.Parallel()

	valid := isSubscriptionCommand([]string{"SUBSCRIBE sidekiq-config"})
	assert.True(t, valid)
}

func TestIsSubscriptionMessageWithMultipleMsg(t *testing.T) {
	t.Parallel()

	invalid := isSubscriptionCommand([]string{"GET", "SUBSCRIBE"})
	assert.False(t, invalid)
}

func TestIsSubscriptionMessageWithNonSubMsg(t *testing.T) {
	t.Parallel()

	invalid := isSubscriptionCommand([]string{"GET"})
	assert.False(t, invalid)
}

func TestHandleNonSubCommand(t *testing.T) {
	t.Parallel()

	conns, _, _, _, _ := createConnectionMocks(t, 1)
	c := conns[0]

	wm := []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("GET")),
			redis.NewBulkBytes([]byte("hi")),
		}),
	}

	err := handleSubscription(c, wm)
	assert.Error(t, err)
}

func TestMultipleChannels(t *testing.T) {
	t.Parallel()

	conns, _, _, _, _ := createConnectionMocks(t, 1)
	c := conns[0]

	wm := []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("SUBSCRIBE")),
			redis.NewBulkBytes([]byte("chatter")),
			redis.NewBulkBytes([]byte("gossip")),
		}),
	}

	err := handleSubscription(c, wm)
	assert.Error(t, err)
}

func TestNewSubscriptionCreated(t *testing.T) {
	t.Parallel()

	conns, _, _, _, _ := createConnectionMocks(t, 1)
	c := conns[0]
	wm := getDefaultSubMessage()

	err := handleSubscription(c, wm)
	assert.NoError(t, err)

	// Ensure a subscription was created
	res := c.reservations.get("c", testChannelName)
	sub := res.(*subscription)
	defer sub.close()
	assert.NotNil(t, sub.init)

	assert.Equal(t, 1, len(c.reservations.list))
}

func TestLocalCreated(t *testing.T) {
	t.Parallel()

	// Setup
	conns, _, _, _, _ := createConnectionMocks(t, 1)
	c := conns[0]
	wm := getDefaultSubMessage()

	// Subscribe
	err := handleSubscription(c, wm)
	assert.NoError(t, err)

	// Ensure a subscription was created
	res := c.reservations.get("c", testChannelName)
	sub := res.(*subscription)
	assert.NotNil(t, sub.init)
	defer sub.close()

	// Ensure a local was added
	local := sub.locals[c.id]
	assert.NotNil(t, local)
}

func TestBroadcastMessage(t *testing.T) {
	t.Parallel()

	// Setup
	conns, rs, _, msgr, _ := createConnectionMocks(t, 2)
	wm := getDefaultSubMessage()

	// Subscribe. Once these are subscribed they'll start reading messages off of the mock messenger
	// and writing the fake messages to the locals.
	err := handleSubscription(conns[0], wm)
	assert.NoError(t, err)
	err = handleSubscription(conns[1], wm)
	assert.NoError(t, err)
	sub := rs.get("c", testChannelName).(*subscription)
	defer sub.close()

	// Ensure we have 2 locals subscribed
	assert.Equal(t, 2, len(sub.locals))

	// Make sure messages were written to both locals
	ok := msgr.messagesExist(createLocalAddressForMock(0))
	assert.True(t, ok)
	ok = msgr.messagesExist(createLocalAddressForMock(1))
	assert.True(t, ok)
}

func TestUnsubscribeLocal(t *testing.T) {
	t.Parallel()

	// Setup
	subMsg := getDefaultSubMessage()
	unsubMsg := unsubscribeMessage(testChannelName)

	conns, rs, _, msgr, _ := createConnectionMocks(t, 2)
	msgr.response = nil

	// Subscribe
	err := handleSubscription(conns[0], subMsg)
	assert.NoError(t, err)
	err = handleSubscription(conns[1], subMsg)
	assert.NoError(t, err)
	sub := rs.get("c", testChannelName).(*subscription)
	sub.init = unsubMsg
	defer sub.close()

	// Unsubscribe 1 local
	assert.Equal(t, 2, len(sub.locals))
	err = sub.unsubscribe(conns[0], nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(sub.locals))

	// Ensure unsubscribe message was written to local
	localMsgs := msgr.peekMessages(createLocalAddressForMock(0))
	foundUnsubMsg := false

	for i := 0; i < len(localMsgs); i++ {
		if len(localMsgs[i]) > 0 && redisArraysEqual(localMsgs[i], unsubMsg) {
			foundUnsubMsg = true
		}
	}

	assert.True(t, foundUnsubMsg)
}

func TestUnsubscribeUpstream(t *testing.T) {
	t.Parallel()

	// Setup
	wm := getDefaultSubMessage()
	conns, rs, sm, msgr, _ := createConnectionMocks(t, 2)
	msgr.response = nil

	// Subscribe
	err := handleSubscription(conns[0], wm)
	assert.NoError(t, err)
	err = handleSubscription(conns[1], wm)
	assert.NoError(t, err)
	sub := rs.get("c", testChannelName).(*subscription)

	// Ensure upstream was returned to pool
	unsubMsg := unsubscribeMessage(testChannelName)
	assert.False(t, sm.connWrapperMock.returned)
	err = sub.unsubscribe(conns[0], nil)
	assert.NoError(t, err)
	err = sub.unsubscribe(conns[1], unsubMsg)
	assert.NoError(t, err)
	assert.True(t, sm.connWrapperMock.returned)

	// Ensure unsub message was sent to upstream
	upMsgs := msgr.peekMessages("localhost:27017")
	foundUnsubMsg := false

	for i := 0; i < len(upMsgs); i++ {
		if len(upMsgs[i]) > 0 && redisArraysEqual(upMsgs[i], unsubMsg) {
			foundUnsubMsg = true
		}
	}

	assert.True(t, foundUnsubMsg)
}

/* Helpers */
func getDefaultSubMessage() []*redis.Message {
	return []*redis.Message{
		redis.NewArray([]*redis.Message{
			redis.NewBulkBytes([]byte("SUBSCRIBE")),
			redis.NewBulkBytes([]byte(testChannelName)),
		}),
	}
}

func redisArraysEqual(arr1 []*redis.Message, arr2 []*redis.Message) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	for i := 0; i < len(arr1); i++ {
		if len(arr1[i].Value) != len(arr2[i].Value) {
			return false
		}

		for j := 0; j < len(arr1[i].Value); j++ {
			if arr1[i].Value[j] != arr2[i].Value[j] {
				return false
			}
		}
	}

	return true
}
