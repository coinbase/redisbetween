package proxy

import (
	"github.cbhq.net/engineering/redisbetween/redis"
	"github.com/coocood/freecache"
)

type Cache struct {
	c *freecache.Cache
}

func NewCache() *Cache {
	return &Cache{
		c: freecache.NewCache(100 * 1024 * 1024), // 100MB, allocated up front
	}
}

// GET and MGET are cacheable, so `Set` and `Get` deal with single values and arrays alike

func (c *Cache) Set(keys [][]byte, m *redis.Message) {
	if m.IsError() { // could be MOVED, etc
		return
	}
	if m.IsArray() {
		for i, mm := range m.Array { // recurse to handle nested responses (eg, MGET)
			c.Set([][]byte{keys[i]}, mm)
		}
	} else {
		c.set(keys[0], m)
	}
}

func (c *Cache) Get(key []byte) (*redis.Message, error) {
	cached, err := c.c.Get(key)
	if err != nil {
		return nil, err
	}
	return redis.DecodeFromBytes(cached)
}

func (c *Cache) GetAll(keys [][]byte) ([]*redis.Message, error) {
	cachedMsgs := make([]*redis.Message, len(keys))
	for i, kk := range keys {
		cached, err := c.Get(kk)
		if err != nil {
			return nil, err
		}
		cachedMsgs[i] = cached
	}
	return cachedMsgs, nil
}

func (c *Cache) Del(key []byte) bool {
	return c.c.Del(key)
}

func (c *Cache) Clear() {
	c.c.Clear()
}

func (c *Cache) set(key []byte, mm *redis.Message) {
	b, _ := redis.EncodeToBytes(mm) // TODO log this error
	_ = c.c.Set(key, b, 360)        // TODO make this TTL configurable, log this error
}
