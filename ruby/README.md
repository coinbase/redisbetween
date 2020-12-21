# Redisbetween

Client patches for the [ruby redis driver](https://github.com/redis/redis-rb).

### Example Usage

```ruby
require 'redis'
require 'redisbetween'

cluster = Redis.new(cluster: ["redis://127.0.0.1:7000"], convert_to_redisbetween_socket: true)
puts cluster.get("hello cluster")

standalone = Redis.new(url: "redis://127.0.0.1:7006", convert_to_redisbetween_socket: true)
puts standalone.get("hello redis")
```

When `require`d in a project, it adds two new options to `Redis.new`.

#### `:convert_to_redisbetween_socket`

Accepts a boolean or a proc.

- Calling `Redis.new` with `url` or `host` & `port` options pointing to a remote redis deployment will return a redis client pointed at a local unix socket path instead. The name of the socket will be derived from the host, port and path given. By default the socket path will be `/var/tmp/redisbetween-#{host}-#{port}-#{path}.sock` but this can be customized by passing a proc to `:convert_to_redisbetween_socket`. The proc will be called with 3 arguments (`host`, `port` and `path`).

- This option will cause pipelined commands to be transmitted with an extra start signal message prepended, and an end signal message appended. Redisbetween requires these signal messages in order to support pipelined commands.

#### `:handle_unsupported_redisbetween_commands`

Accepts a proc which is called when an unsupported command is called. When undefined, does not change client behavior. Use this to log before rolling out redisbetween, or to raise in specs.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'redisbetween'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install redisbetween

## Development

The tests perform an integration test against a running redis cluster and instance of redisbetween. Use `make ruby-test` in this repo's Makefile to run them.
