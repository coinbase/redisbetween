package handlers

var UnsupportedCommands = map[string]bool{
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
	"XINFO":        true, // streams
	"XGROUP":       true, // streams
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

const (
	TransactionOpen = iota
	TransactionInner
	TransactionClose
)

var TransactionCommands = map[string]int{
	// redis transactions are stateful on the server side, and are associated with
	// the connection, so we can only allow them within a set of pipelined commands.
	// this map helps us keep track whether or not we are inside a transaction.
	"DISCARD": TransactionClose,
	"EXEC":    TransactionClose,
	"MULTI":   TransactionOpen,
	"UNWATCH": TransactionInner,
	"WATCH":   TransactionOpen,
}
