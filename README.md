# redis-proxy

This is a connection pooling proxy for redis. It was originally built because the Monorail was hitting AWS elasticache
connection limits against its redis clusters. Its purpose is to solve a specific problem: many application processes
that cannot otherwise share a connection pool need to connect to a single redis cluster.

redis-proxy supports both standalone and clustered redis deployments with the following caveats:

- **Blocking Commands** that cause the client to hold a connection open such as `BLPOP`, `BRPOPLPUSH`, `SUBSCRIBE` and
`WAIT` are not allowed by redis-proxy because of the risk of exhausting the connection pool. redis-proxy is not a good
solution for clients like sidekiq that rely heavily on these blocking commands.

- **Transactions** are not supported. This means that the commands `DISCARD`, `EXEC`, `MULTI`, `UNWATCH` and `WATCH`
will return errors from the proxy before they reach an upstream host. This is because redis stores state about open
transactions on the server side, attached to each client connection. In order to support transactions, each one would
have to pin a connection from the pool to a single client while a transaction were open. redis-proxy disables these
commands to avoid cases where applications could accidentally exhaust the connection pool.

- **Pipelines** are not supported. It may be possible to add support for pipelines to redis-proxy in the future.

- The **SELECT** command, which is used by redis clients when connecting to a db other than the default `0`, is not
allowed. However, redis-proxy _does_ support multiple dbs by specifying the db number in the endpoint url path. With an
example URL of `redis://example.com/3`, the resulting connection pool would be mapped to the socket path
`/var/tmp/redis-proxy-example.com-3.sock` suffix, and all connections would issue a `SELECT 3` command before entering
the pool. Note that each db number gets its own connection pool, so adjust `maxpoolsize` accordingly when using this
feature.

- The **AUTH** command is not supported. We currently don't use it in our apps. If this is needed in the future, we
could add support by pre-emptively sending the AUTH command on all new connections, like we do with `SELECT`.

### How it works

redis-proxy creates a connection pool for each upstream redis server it discovers (either via configuration at start
time, snooping on `CLUSTER` commands or via `ASK`/`MOVED` errors) and maps a local unix socket to that pool.
Applications running on the same host can connect to redis via this unix socket instead of connecting directly to the
redis server, thus sharing a relatively smaller number of connections among the many processes on a machine.

Upon startup, redis-proxy creates a pool of connections to the redis endpoint provided and listens on a unix socket
named after the endpoint. By default, it will be named `/var/tmp/redis-proxy-${host}-${port}(-${db}).sock`. This can be
customized using the `-localsocketprefix` and `-localsocketsuffix` options. For standalone redis deployments, this will
be the only socket created. However, redis-proxy will inspect responses to `CLUSTER` commands, looking for references to
cluster members that it hasn't yet seen. When it sees a new cluster member, it allocates a new connection pool and unix
socket for it before relaying the response to the client.

**Important** - Redis clients must be patched to translate URLs to local sockets according to the naming scheme
*described above. For example, here's a patch to the ruby redis client:

```ruby
module RedisPatch
  def initialize(options = {})
    if options[:convert_to_redis_proxy_socket]
      u = URI(options[:url])
      path = u.path.empty? ? nil : u.path.delete_prefix('/')
      u.path = ['/var/tmp/redis-proxy', u.host, u.port, path].compact.join('-') + '.sock'
      u.host = nil
      u.port = nil
      u.scheme = 'unix'
      puts "new url ->", u.to_s
      options[:url] = u.to_s
    end
    super(options)
  end
end

Redis::Client.prepend(RedisPatch)

cluster = Redis.new(cluster: [{ host: 'some.redis.cluster.com' }], convert_to_redis_proxy_socket: true)
standalone = Redis.new(url: 'redis://some.redis.server.com/3', convert_to_redis_proxy_socket: true)
puts cluster.cluster("slots")
puts standalone.get("hi")
```

Here's an example of a similar patch to the go-redis client. Note that this one does not handle db number selection, as
that is not supported by redis cluster anyway.

```go
opt := &redis.ClusterOptions{
    Addrs: []string{address},
    Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
        if strings.Contains(network, "tcp") {
            host, port, err := net.SplitHostPort(addr)
            if err != nil {
                return nil, err
            }
            addr = "/var/tmp/redis-proxy-" + host + "-" + port + ".sock"
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
go install github.cbhq.net/engineering/redis-proxy
```

### Usage
```
Usage: bin/redis-proxy [OPTIONS] uri1 [uri2] ...
  -localsocketprefix string
    	prefix to use for unix socket filenames (default "/var/tmp/redis-proxy-")
  -localsocketsuffix string
    	suffix to use for unix socket filenames (default ".sock")
  -loglevel string
    	one of: debug, info, warn, error, dpanic, panic, fatal (default "info")
  -network string
    	one of: tcp, tcp4, tcp6, unix or unixpacket (default "unix")
  -pretty
    	pretty print logging
  -readtimeout duration
    	read timeout (default 1s)
  -statsd string
    	statsd address (default "localhost:8125")
  -unlink
    	unlink existing unix sockets before listening
  -writetimeout duration
    	write timeout (default 1s)
```

Each URI can specify the following settings as GET params:

- `minpoolsize` sets the min connection pool size for this host. Defaults to 1
- `maxpoolsize` sets the max connection pool size for this host. Defaults to 10
- `cluster` must be set to `true` for cluster upstreams. Defaults to `false`
- `label` optionally tags events and metrics for proxy activity on this host or cluster. Defaults to `""` (disabled)

### Statsd

TBD
