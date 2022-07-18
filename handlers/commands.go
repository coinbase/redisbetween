package handlers

import "strings"

var ReadCommands = map[string]bool{
	"BITCOUNT":             true,
	"BITFIELD_RO":          true,
	"BITOP":                true,
	"BITPOS":               true,
	"DBSIZE":               true,
	"EVAL_RO":              true,
	"EVALSHA_RO":           true,
	"EXISTS":               true,
	"EXPIRETIME":           true,
	"GEODIST":              true,
	"GEOHASH":              true,
	"GEOPOS":               true,
	"GEORADIUS_RO":         true,
	"GEORADIUSBYMEMBER_RO": true,
	"GEOSEARCH":            true,
	"GET":                  true,
	"GETBIT":               true,
	"GETRANGE":             true,
	"HEXISTS":              true,
	"HGET":                 true,
	"HGETALL":              true,
	"HKEYS":                true,
	"HLEN":                 true,
	"HMGET":                true,
	"HRANDFIELD":           true,
	"HSCAN":                true,
	"HSTRLEN":              true,
	"HVALS":                true,
	"KEYS":                 true,
	"LCS":                  true,
	"LINDEX":               true,
	"LLEN":                 true,
	"LPOS":                 true,
	"LRANGE":               true,
	"MGET":                 true,
	"PEXPIRETIME":          true,
	"PFCOUNT":              true,
	"PTTL":                 true,
	"SCAN":                 true,
	"SCARD":                true,
	"SINTER":               true,
	"SINTERCARD":           true,
	"SISMEMBER":            true,
	"SMEMBERS":             true,
	"SMISMEMBER":           true,
	"SORT_RO":              true,
	"SSCAN":                true,
	"STRLEN":               true,
	"SUBSTR":               true,
	"SUNION":               true,
	"TIME":                 true,
	"TTL":                  true,
	"TYPE":                 true,
	"ZCARD":                true,
	"ZCOUNT":               true,
	"ZDIFF":                true,
	"ZINTER":               true,
	"ZINTERCARD":           true,
	"ZLEXCOUNT":            true,
	"ZMSCORE":              true,
	"ZRANDMEMBER":          true,
	"ZRANGE":               true,
	"ZRANGEBYLEX":          true,
	"ZRANGEBYSCORE":        true,
	"ZRANK":                true,
	"ZREVRANGE":            true,
	"ZREVRANGEBYLEX":       true,
	"ZREVRANGEBYSCORE":     true,
	"ZREVRANK":             true,
	"ZSCAN":                true,
	"ZSCORE":               true,
	"ZUNION":               true,
}

var WriteCommands = map[string]bool{
	"APPEND":            true,
	"BITFIELD":          true,
	"COPY":              true,
	"DECR":              true,
	"DECRBY":            true,
	"DEL":               true,
	"EVAL":              true,
	"EVALSHA":           true,
	"EXPIRE":            true,
	"EXPIREAT":          true,
	"FCALL":             true,
	"FLUSHALL":          true,
	"FLUSHDB":           true,
	"GEOADD":            true,
	"GEORADIUS":         true,
	"GEORADIUSBYMEMBER": true,
	"GEOSEARCHSTORE":    true,
	"GETDEL":            true,
	"GETEX":             true,
	"GETSET":            true,
	"HDEL":              true,
	"HINCRBY":           true,
	"HINCRBYFLOAT":      true,
	"HMSET":             true,
	"HSETNX":            true,
	"INCR":              true,
	"INCRBY":            true,
	"INCRBYFLOAT":       true,
	"LINSERT":           true,
	"LMOVE":             true,
	"LPOP":              true,
	"LMPOP":             true,
	"LPUSH":             true,
	"LPUSHX":            true,
	"LREM":              true,
	"LSET":              true,
	"LTRIM":             true,
	"MOVE":              true,
	"MSET":              true,
	"MSETNX":            true,
	"PERSIST":           true,
	"PEXPIRE":           true,
	"PEXPIREAT":         true,
	"PEXPIRETIME":       true,
	"PFADD":             true,
	"PFMERGE":           true,
	"PSETX":             true,
	"RENAME":            true,
	"RENAMENX":          true,
	"RPOP":              true,
	"RPUSH":             true,
	"RPUSHX":            true,
	"SADD":              true,
	"SET":               true,
	"SETBIT":            true,
	"SETEX":             true,
	"SETNX":             true,
	"SETRANGE":          true,
	"SINTERSTORE":       true,
	"SMOVE":             true,
	"SORT":              true,
	"SPOP":              true,
	"SREM":              true,
	"SUNIONSTORE":       true,
	"TOUCH":             true,
	"UNLINK":            true,
	"ZADD":              true,
	"ZDIFFSTORE":        true,
	"ZINCRBY":           true,
	"ZINTERSTORE":       true,
	"ZMPOP":             true,
	"ZPOPMAX":           true,
	"ZPOPMIN":           true,
	"ZRANGESTORE":       true,
	"ZREM":              true,
	"ZREMRANGEBYLEX":    true,
	"ZREMRANGEBYRANK":   true,
	"ZREMRANGEBYSCORE":  true,
	"ZUNIONSTORE":       true,
}

var UnsupportedCommands = map[string]bool{
	// blocking commands cause clients to hold a connection open and wait for data to
	// appear. these monopolize a connection from the pool, so don't make sense to
	// allow for a connection pooling proxy. if these are required in the future, we
	// could allow ad-hoc connections to be allocated in addition to the pool to
	// support these?
	"BLPOP":      true,
	"BLMPOP":     true,
	"BLMOVE":     true,
	"BRPOP":      true,
	"BZPOPMAX":   true,
	"BZMPOP":     true,
	"BZPOPMIN":   true,
	"XREAD":      true, // streams
	"XREADGROUP": true, // streams
	"WAIT":       true,

	// these commands also store connection state on the server, and so won't work
	// with redisbetween without some special work to support them
	"AUTH":   true,
	"SELECT": true,
}

const (
	TransactionOpen = iota
	TransactionInner
	TransactionClose
)

var TransactionCommands = map[string]int{
	// redis transactions are stateful on the server side, and are associated with
	// the connection, so we can only allow them within a set of pipelined commands.
	// this map helps us keep track of whether we are inside a transaction.
	"DISCARD": TransactionClose,
	"EXEC":    TransactionClose,
	"MULTI":   TransactionOpen,
	"UNWATCH": TransactionInner,
	"WATCH":   TransactionOpen,
}

// Checks if command is a) singular and b) is a *blocking command
func isBlockingCommand(commands []string) bool {
	if len(commands) == 1 && strings.Contains(commands[0], "BRPOPLPUSH") {
		return true
	}

	return false
}

// Checks if command is a) singular and b) is a *subscribe command
func isSubscriptionCommand(commands []string) bool {
	if len(commands) == 1 && strings.Contains(commands[0], "SUBSCRIBE") {
		return true
	}

	return false
}

// Checks if command is a read command
func isReadCommand(commands []string) bool {
	if _, ok := ReadCommands[commands[0]]; ok {
		return true
	}
	return false
}

// Checks if command is a write command
func isWriteCommand(commands []string) bool {
	if _, ok := WriteCommands[commands[0]]; ok {
		return true
	}
	return false
}
