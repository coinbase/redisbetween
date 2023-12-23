# redisbetween

This is a connection pooling proxy for redis. It was originally built for an application that was hitting 
connection limits against its redis clusters. Its purpose is to solve a specific problem: many application processes
that cannot otherwise share a connection pool need to connect to a single redis cluster.

redisbetween supports both standalone and clustered redis deployments with the following caveats:

- **Blocking Commands** that cause the client to hold a connection open are partially supported, with limitations.
Specifically, `BRPOPLPUSH` and `SUBSCRIBE`/`PSUBSCRIBE` are supported in non-clustered mode. In order to prevent
exhausting the connection pool, these commands are implemented using reserved connections, configured via
`maxsubscriptions` and `maxblockers` (both default to 1). The supported commands are specifically intended to support
sidekiq servers with reliable fetch. Other blocking commands such as `BLPOP` or `WAIT` are not yet supported.

- **Pipelines** are supported, but require a client patch. Normally, redis clients may send multiple commands
back-to-back before reading a batch of responses all at once from the server. Since redisbetween shares upstream
connections among many clients, it relies on special "signal" messages to indicate the beginning and end of batched
commands. Clients using redisbetween must prepend a `GET 🔜` and append a `GET 🔚` to their batch of messages in order
for redisbetween to properly proxy the pipelined commands and responses.

- **Transactions are only supported _within pipelines_.** This means that the commands `DISCARD`, `EXEC`, `MULTI`,
`UNWATCH` and `WATCH` will return errors from the proxy before they reach an upstream host unless they occur between the
two signal values described above in the **Pipelines** section. This is because redis stores state about open
transactions on the server side, attached to each client connection. In order to support transactions without
connection-pinning, we require that the full set of operations be sent in one batch so that the connection we check back
into the pool does not leak state to other clients.

- The **SELECT** command, which is used by redis clients when connecting to a db other than the default `0`, is not
allowed. However, redisbetween _does_ support multiple dbs by specifying the db number in the endpoint url path. With an
example URL of `redis://example.com/3`, the resulting connection pool would be mapped to the socket path
`/var/tmp/redisbetween-example.com-3.sock` suffix, and all connections would issue a `SELECT 3` command before entering
the pool. Note that each db number gets its own connection pool, so adjust `maxpoolsize` accordingly when using this
feature.

- The **AUTH** command is not supported. If this is needed in the future, we
could add support by pre-emptively sending the AUTH command on all new connections, like we do with `SELECT`.

### How it works

redisbetween creates a connection pool for each upstream redis server it discovers (either via configuration at start
time, snooping on `CLUSTER` commands or via `ASK`/`MOVED` errors) and maps a local unix socket to that pool.
Applications running on the same host can connect to redis via this unix socket instead of connecting directly to the
redis server, thus sharing a relatively smaller number of connections among the many processes on a machine.

Upon startup, redisbetween creates a pool of connections to the redis endpoint provided and listens on a unix socket
named after the endpoint. By default, it will be named `/var/tmp/redisbetween-${host}-${port}(-${db})(-ro).sock`. This can be
customized using the `-localsocketprefix` and `-localsocketsuffix` options. For standalone redis deployments, this will
be the only socket created. However, redisbetween will inspect responses to `CLUSTER` commands, looking for references to
cluster members that it hasn't yet seen. When it sees a new cluster member, it allocates a new connection pool and unix
socket for it before relaying the response to the client.

### Redisbetween Gem

The [ruby](/ruby) directory contains a ruby gem that monkey patches the ruby redis client to support redisbetween. See
the [readme](/ruby/README.md) for more details.

Here's an example of a patch to the go-redis client. Note that this one does not handle db number selection, as that is
not supported by redis cluster anyway.

```go
readonly := true
opt := &redis.ClusterOptions{
    Addrs: []string{address},
    Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
        if strings.Contains(network, "tcp") {
            host, port, err := net.SplitHostPort(addr)
            if err != nil {
                return nil, err
            }
            addr = "/var/tmp/redisbetween-" + host + "-" + port
            if readonly {
                addr += "-ro"
            }
            addr += ".sock"
            network = "unix"
        }
        return net.Dial(network, addr)
    },
}
client := redis.NewClusterClient(opt)
res := client.Do(context.Background(), "ping")
```

### Installation
```
go install github.com/d2army/redisbetween
```

### Usage
```
Usage: bin/redisbetween [OPTIONS] uri1 [uri2] ...
  -localsocketprefix string
    	prefix to use for unix socket filenames (default "/var/tmp/redisbetween-")
  -localsocketsuffix string
    	suffix to use for unix socket filenames (default ".sock")
  -loglevel string
    	one of: debug, info, warn, error, dpanic, panic, fatal (default "info")
  -network string
    	one of: tcp, tcp4, tcp6, unix or unixpacket (default "unix")
  -pretty
    	pretty print logging
  -statsd string
    	statsd address (default "localhost:8125")
  -unlink
    	unlink existing unix sockets before listening
  -healthcheck
      start a background process to check the health of server connections
  -healthcheckcycle
      duration after which the healthcheck process should repeat itself (default 60s)
  -healthcheckthreshold
      count of consecutive healthcheck failures after which a server is declared unhealthy (default 3)
  -idletimeout
      how long can an inactive connection remain idle in the pool (default 0 meaning no timeout)
```

Each URI can specify the following settings as GET params:

- `minpoolsize` sets the min connection pool size for this host. Defaults to 1
- `maxpoolsize` sets the max connection pool size for this host. Defaults to 10
- `label` optionally tags events and metrics for proxy activity on this host or cluster. Defaults to `""` (disabled)
- `readtimeout` timeout for reads to this upstream. Defaults to 5s
- `writetimeout` timeout for writes to this upstream. Defaults to 5s
- `readonly` every connection issues a [READONLY](https://redis.io/commands/readonly) command before entering the pool. Defaults to false
- `maxsubscriptions` sets the max number of channels that can be subscribed to at one time. Defaults to 1.
- `maxblockers` sets the max number of commands that can be blocking at one time. Defaults to 1. 
- `idletimeout` how long can an inactive connection remain idle in the pool. Default is the global level `idletimeout` or 0 (no timeout)

Example: `./redisbetween -unlink -pretty -loglevel debug redis://localhost:7001?maxsubscriptions=2&maxblockers=2`
