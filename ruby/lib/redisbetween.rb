require 'redisbetween/version'
require 'redis'
require 'uri'

module Redisbetween
  class Error < StandardError; end

  module ClientPatch
    attr_reader :redisbetween_enabled

    def initialize(options = {})
      if options[:convert_to_redisbetween_socket]
        @redisbetween_enabled = true
        if options[:url]
          u = URI(options[:url])
          if u.scheme != 'unix'
            path = u.path.empty? ? nil : u.path.delete_prefix('/')
            u.path = Redisbetween.socket_path(options[:convert_to_redisbetween_socket], u.host, u.port, path)
            u.host = nil
            u.port = nil
            u.scheme = 'unix'
            options[:url] = u.to_s
          end
        elsif options[:host] && options[:port] && options[:scheme] != 'unix'
          path = Redisbetween.socket_path(options[:convert_to_redisbetween_socket], options[:host], options[:port])
          options[:url] = "unix:#{path}"
          [:port, :host, :scheme].each { |k| options[k] = nil }
        end
      end
      @redisbetween_pipeline_signals_enabled = !!options[:redisbetween_pipeline_signals_enabled] || @redisbetween_enabled
      super(options)
    end

    def call_pipelined(pipeline)
      if @redisbetween_pipeline_signals_enabled
        pipeline.futures.unshift(Redis::Future.new([:get, "🔜"], nil, nil))
        pipeline.futures << Redis::Future.new([:get, "🔚"], nil, nil)
      end
      super
    end
  end

  module RedisPatch
    attr_reader :redisbetween_enabled

    def initialize(options = {})
      @redisbetween_enabled = !!options[:convert_to_redisbetween_socket]
      @disallow_unsupported_redisbetween_commands = !!options[:disallow_unsupported_redisbetween_commands] || @redisbetween_enabled
      super(options)
    end

    def multi(*args, &block)
      if @disallow_unsupported_redisbetween_commands && !block_given?
        raise Error.new("redisbetween requires that `multi` always be called with a block")
      end
      super
    end

    UNSUPPORTED_COMMANDS = [
      :auth,
      :blpop,
      :brpop,
      :brpoplpush,
      :bzpopmax,
      :bzpopmin,
      :psubscribe,
      :punsubscribe,
      :select,
      :subscribe,
      :unsubscribe,
      :wait,
      :xread,
      :xreadgroup,
    ].to_set.freeze

    UNSUPPORTED_COMMANDS.each do |command|
      define_method(command) do |*args|
        if @disallow_unsupported_redisbetween_commands
          raise Error.new("unsupported command #{command}")
        end
        super *args
      end
    end
  end

  def self.socket_path(option, host, port, path = nil)
    if option.respond_to?(:call)
      option.call(host, port, path)
    else
      default_socket_path(host, port, path)
    end
  end

  def self.default_socket_path(host, port, path = nil)
    ['/var/tmp/redisbetween', host, port, path].compact.join('-') + '.sock'
  end
end

Redis.prepend(Redisbetween::RedisPatch)
Redis::Client.prepend(Redisbetween::ClientPatch)
