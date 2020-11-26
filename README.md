# redis-proxy

TBD

### How it works

TBD

### Installation
```
go install github.cbhq.net/engineering/redis-proxy
```

### Usage
```
Usage: redis-proxy [OPTIONS] address1=uri1 [address2=uri2] ...
  -loglevel string
    	One of: debug, info, warn, error, dpanic, panic, fatal (default "info")
  -network string
    	One of: tcp, unix (default "tcp")
  -statsd string
    	Statsd address (default "localhost:8125")
  -unlink
    	Unlink existing unix sockets before listening
```

TODO mention non-standard `cluster=true` URL param

The `label` query parameter in the connection URI is used to any tag statsd metrics or logs for that connection.


### Statsd

TBD

### Background

TBD
