package proxy

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/mediocregopher/radix/v3"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"testing"
)

//var (
//ctx       = context.Background()
//proxyPort = 6379
//proxyURI  = fmt.Sprintf("redis://localhost:%d/0", proxyPort)
//)

//type Trainer struct {
//	Name string
//	Age  int
//	City string
//}

func TestProxy(t *testing.T) {
	proxy := setupProxy(t)

	go func() {
		err := proxy.Run()
		assert.Nil(t, err)
	}()

	client := setupClient(t)
	//collection := client.Database("test").Collection("trainers")
	//_, err := collection.DeleteMany(ctx, bson.D{{}})

	err := client.Do(radix.Cmd(nil, "DEL", "hello"))
	assert.Nil(t, err)

	//ash := Trainer{"Ash", 10, "Pallet Town"}
	//misty := Trainer{"Misty", 10, "Cerulean City"}
	//brock := Trainer{"Brock", 15, "Pewter City"}

	//_, err = collection.InsertOne(ctx, ash)
	//assert.Nil(t, err)

	err = client.Do(radix.Cmd(nil, "SET", "hello", "world"))
	assert.Nil(t, err)

	//filter := bson.D{{Key: "name", Value: "Ash"}}
	//update := bson.D{
	//	{Key: "$inc", Value: bson.D{
	//		{Key: "age", Value: 1},
	//	}},
	//}
	//updateResult, err := collection.UpdateOne(ctx, filter, update)
	//assert.Nil(t, err)
	//assert.Equal(t, int64(1), updateResult.MatchedCount)
	//assert.Equal(t, int64(1), updateResult.ModifiedCount)

	rcv := ""
	err = client.Do(radix.Cmd(&rcv, "GET", "hello"))
	assert.Nil(t, err)
	assert.Equal(t, "world", rcv)

	//var result Trainer
	//err = collection.FindOne(ctx, filter).Decode(&result)
	//assert.Nil(t, err)
	//assert.Equal(t, "Pallet Town", result.City)

	//var results []Trainer
	//cur, err := collection.Find(ctx, bson.D{}, options.Find().SetLimit(2).SetBatchSize(1))
	//assert.Nil(t, err)
	//err = cur.All(ctx, &results)
	//assert.Nil(t, err)
	//assert.Equal(t, "Pallet Town", results[0].City)
	//assert.Equal(t, "Cerulean City", results[1].City)
	//
	//deleteResult, err := collection.DeleteMany(ctx, bson.D{{}})
	//assert.Nil(t, err)
	//assert.Equal(t, int64(3), deleteResult.DeletedCount)

	err = client.Close()
	assert.Nil(t, err)

	proxy.Shutdown()
}

//func TestProxyUnacknowledgedWrites(t *testing.T) {
//	proxy := setupProxy(t)
//	defer proxy.Shutdown()
//
//	go func() {
//		err := proxy.Run()
//		assert.Nil(t, err)
//	}()
//
//	// Create a client with retryable writes disabled so the test will fail if the proxy crashes while processing the
//	// unacknowledged write. If the proxy were to crash, it would close all connections and the next write would error
//	// if retryable writes are disabled.
//	clientOpts := options.Client().SetRetryWrites(false)
//	client := setupClient(t, clientOpts)
//	defer func() {
//		err := client.Disconnect(ctx)
//		assert.Nil(t, err)
//	}()
//
//	// Create two *Collection instances: one for setup and basic operations and and one configured with an
//	// unacknowledged write concern for testing.
//	wc := writeconcern.New(writeconcern.W(0))
//	setupCollection := client.Database("test").Collection("trainers")
//	unackCollection, err := setupCollection.Clone(options.Collection().SetWriteConcern(wc))
//	assert.Nil(t, err)
//
//	// Setup by deleteing all documents.
//	_, err = setupCollection.DeleteMany(ctx, bson.D{})
//	assert.Nil(t, err)
//
//	ash := Trainer{"Ash", 10, "Pallet Town"}
//	_, err = unackCollection.InsertOne(ctx, ash)
//	assert.Equal(t, mongo.ErrUnacknowledgedWrite, err) // driver returns a special error value for w=0 writes
//
//	// Insert a document using the setup collection and ensure document count is 2. Doing this ensures that the proxy
//	// did not crash while processing the unacknowledged write.
//	_, err = setupCollection.InsertOne(ctx, ash)
//	assert.Nil(t, err)
//
//	count, err := setupCollection.CountDocuments(ctx, bson.D{})
//	assert.Nil(t, err)
//	assert.Equal(t, int64(2), count)
//}

func setupProxy(t *testing.T) *Proxy {
	t.Helper()

	uri := "redis://localhost:6379"
	if os.Getenv("CI") == "true" {
		uri = "redis://redis:6379"
	}

	sd, err := statsd.New("localhost:8125")
	assert.Nil(t, err)

	proxy, err := NewProxy(zap.L(), sd, "test", "tcp", uri, ":6380", false, 5)
	assert.Nil(t, err)
	return proxy
}

func setupClient(t *testing.T) *radix.Pool {
	t.Helper()

	pool, err := radix.NewPool("tcp", "redis://localhost:6379", 5)
	assert.Nil(t, err)

	err = pool.Do(radix.Cmd(nil, "PING"))
	if err != nil {
		_ = pool.Close()
		// Use t.Fatalf instead of assert because we want to fail fast if the cluster is down.
		t.Fatalf("error pinging redis: %v", err)
	}

	return pool
}
