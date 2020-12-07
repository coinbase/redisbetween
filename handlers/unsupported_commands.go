package handlers

var UnsupportedCommands = map[string]bool{
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
	"BLPOP":        true,
	"BRPOP":        true,
	"BRPOPLPUSH":   true,
	"BZPOPMAX":     true,
	"BZPOPMIN":     true,
	"XREAD":        true, // streams
	"XREADGROUP":   true, // streams
	"SUBSCRIBE":    true,
	"UNSUBSCRIBE":  true,
	"WAIT":         true,
	"PUNSUBSCRIBE": true,
	"PSUBSCRIBE":   true,

	// these commands also store connection state on the server, and so won't work
	// with redisbetween without some special work to support them
	"AUTH":   true,
	"SELECT": true,
}
