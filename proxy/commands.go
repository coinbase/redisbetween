package proxy

var SupportedCommands = map[string]bool{
	"APPEND":               true,
	"BGREWRITEAOF":         true,
	"BGSAVE":               true,
	"BITCOUNT":             true,
	"BITFIELD":             true,
	"BITPOS":               true,
	"COMMAND":              true,
	"DECR":                 true,
	"DECRBY":               true,
	"DUMP":                 true,
	"ECHO":                 true,
	"EVAL":                 true,
	"EVALSHA":              true,
	"EXPIRE":               true,
	"EXPIREAT":             true,
	"FLUSHDB":              true,
	"GEOADD":               true,
	"GEODIST":              true,
	"GEOHASH":              true,
	"GEOPOS":               true,
	"GEORADIUS":            true,
	"GEORADIUSBYMEMBER":    true,
	"GEORADIUSBYMEMBER_RO": true,
	"GEORADIUS_RO":         true,
	"GET":                  true,
	"GETBIT":               true,
	"GETRANGE":             true,
	"GETSET":               true,
	"HDEL":                 true,
	"HEXISTS":              true,
	"HGET":                 true,
	"HGETALL":              true,
	"HINCRBY":              true,
	"HINCRBYFLOAT":         true,
	"HKEYS":                true,
	"HLEN":                 true,
	"HMGET":                true,
	"HMSET":                true,
	"HOST":                 true,
	"HSCAN":                true,
	"HSET":                 true,
	"HSETNX":               true,
	"HSTRLEN":              true,
	"HVALS":                true,
	"INCR":                 true,
	"INCRBY":               true,
	"INCRBYFLOAT":          true,
	"LASTSAVE":             true,
	"LINDEX":               true,
	"LINSERT":              true,
	"LLEN":                 true,
	"LOLWUT":               true, // :D
	"LPOP":                 true,
	"LPUSH":                true,
	"LPUSHX":               true,
	"LRANGE":               true,
	"LREM":                 true,
	"LSET":                 true,
	"LTRIM":                true,
	"MOVE":                 true,
	"MSET":                 true,
	"MSETNX":               true,
	"OBJECT":               true,
	"PERSIST":              true,
	"PEXPIRE":              true,
	"PEXPIREAT":            true,
	"PFADD":                true,
	"PFCOUNT":              true,
	"PFMERGE":              true,
	"PING":                 true,
	"POST":                 true,
	"PSETEX":               true,
	"PTTL":                 true,
	"RANDOMKEY":            true,
	"RENAME":               true,
	"RENAMENX":             true,
	"RESTORE":              true,
	"RPOP":                 true,
	"RPOPLPUSH":            true,
	"RPUSH":                true,
	"RPUSHX":               true,
	"SADD":                 true,
	"SAVE":                 true,
	"SCAN":                 true,
	"SCARD":                true,
	"SDIFF":                true,
	"SET":                  true,
	"SETBIT":               true,
	"SETEX":                true,
	"SETNX":                true,
	"SETRANGE":             true,
	"SINTER":               true,
	"SISMEMBER":            true,
	"SMEMBERS":             true,
	"SMOVE":                true,
	"SORT":                 true,
	"SPOP":                 true,
	"SRANDMEMBER":          true,
	"SREM":                 true,
	"SSCAN":                true,
	"STRLEN":               true,
	"SUBSTR":               true,
	"SUNION":               true,
	"SWAPDB":               true,
	"TTL":                  true,
	"TYPE":                 true,
	"XACK":                 true,
	"XADD":                 true,
	"XCLAIM":               true,
	"XDEL":                 true,
	"XGROUP":               true,
	"XINFO":                true,
	"XLEN":                 true,
	"XPENDING":             true,
	"XRANGE":               true,
	"XREVRANGE":            true,
	"XSETID":               true,
	"XTRIM":                true,
	"ZADD":                 true,
	"ZCARD":                true,
	"ZCOUNT":               true,
	"ZINCRBY":              true,
	"ZLEXCOUNT":            true,
	"ZPOPMAX":              true,
	"ZPOPMIN":              true,
	"ZRANGE":               true,
	"ZRANGEBYLEX":          true,
	"ZRANGEBYSCORE":        true,
	"ZRANK":                true,
	"ZREM":                 true,
	"ZREMRANGEBYLEX":       true,
	"ZREMRANGEBYRANK":      true,
	"ZREMRANGEBYSCORE":     true,
	"ZREVRANGE":            true,
	"ZREVRANGEBYLEX":       true,
	"ZREVRANGEBYSCORE":     true,
	"ZREVRANK":             true,
	"ZSCAN":                true,
	"ZSCORE":               true,

	// cluster does not support multi-key invocations of the following commands
	// TODO implement validation for these to reject invocations with more than one key.
	"DEL":    true,
	"EXISTS": true,
	"KEYS":   true,
	"MGET":   true,
	"TOUCH":  true,
	"UNLINK": true,
}

var UnsupportedCommands = map[string]bool{
	"ACL":          true,
	"ASKING":       true,
	"AUTH":         true, // not used by our apps currently
	"CLIENT":       true,
	"CLUSTER":      true, // this gets intercepted
	"CONFIG":       true,
	"DBSIZE":       true, // doesnt make sense for clusters?
	"DEBUG":        true,
	"FLUSHALL":     true, // todo: investigate why this gets sent to secondaries sometimes
	"HELLO":        true, // RESP3 protocol handshake
	"INFO":         true,
	"LATENCY":      true,
	"MEMORY":       true,
	"MIGRATE":      true,
	"MODULE":       true,
	"MONITOR":      true,
	"PFDEBUG":      true,
	"PFSELFTEST":   true,
	"PSUBSCRIBE":   true,
	"PSYNC":        true,
	"PUBLISH":      true,
	"PUBSUB":       true,
	"PUNSUBSCRIBE": true,
	"READONLY":     true,
	"READWRITE":    true,
	"REPLCONF":     true,
	"REPLICAOF":    true,
	"ROLE":         true,
	"SCRIPT":       true,
	"SELECT":       true, // cluster only supports one db
	"SHUTDOWN":     true,
	"SLAVEOF":      true,
	"SLOWLOG":      true,
	"SUBSCRIBE":    true,
	"SYNC":         true,
	"TIME":         true,
	"UNSUBSCRIBE":  true,
	"WAIT":         true,

	// redis transactions are stateful on the server side, and are associated with
	// the connection. for a connection pooling proxy that shares a single connection
	// among multiple clients, it doesn't make sense to support transactions, as each
	// one monopolizes a connection from the pool as long as it is open, just like
	// blocking commands.
	"DISCARD": true,
	"EXEC":    true,
	"MULTI":   true,
	"UNWATCH": true,
	"WATCH":   true,

	// blocking commands cause clients to hold a connection open and wait for data to
	// appear. these monopolize a connection from the pool, so don't make sense to
	// allow for a connection pooling proxy. if these are required in the future, we
	// could allow ad-hoc connections to be allocated in addition to the pool to
	// support these?
	"BLPOP":      true,
	"BRPOP":      true,
	"BRPOPLPUSH": true,
	"BZPOPMAX":   true,
	"BZPOPMIN":   true,
	"XREAD":      true, // streams
	"XREADGROUP": true, // streams

	// cluster doesn't support multi-key operations that may interact with more than
	// one slot. we might later implement these commands in a way that works
	// _sometimes_ but for now we will disallow them.
	"BITOP":       true,
	"SDIFFSTORE":  true,
	"SINTERSTORE": true,
	"SUNIONSTORE": true,
	"ZINTERSTORE": true,
	"ZUNIONSTORE": true,
}

func KnownCommand(c string) bool {
	return SupportedCommand(c) || UnsupportedCommand(c)
}
func SupportedCommand(c string) bool {
	_, ok := SupportedCommands[c]
	return ok
}
func UnsupportedCommand(c string) bool {
	_, ok := UnsupportedCommands[c]
	return ok
}
