require 'redisbetween/version'
require 'redis'
require 'uri'

module Redisbetween
  class Error < StandardError; end

  PIPELINE_START_SIGNAL = '🔜'
  PIPELINE_END_SIGNAL = '🔚'

  module Common
    attr_reader :redisbetween_enabled

    def redisbetween_options(options)
      @redisbetween_enabled = !!options[:convert_to_redisbetween_socket]
      @handle_unsupported_redisbetween_command = options[:handle_unsupported_redisbetween_commands]
      if @redisbetween_enabled
        @handle_unsupported_redisbetween_command ||= ->(cmd) { puts "redisbetween: unsupported #{cmd}" }
      end
    end
  end

  module ClientPatch
    include Common

    def initialize(options = {})
      redisbetween_options(options)
      if redisbetween_enabled
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
          [:port, :host, :scheme].each { |k| options[k] = nil }
          options[:url] = "unix:#{path}"
        end
      end
      super(options)
    end

    def call_pipeline(pipeline)
      if redisbetween_enabled
        pipeline.futures.unshift(Redis::Future.new([:get, PIPELINE_START_SIGNAL], nil, nil))
        pipeline.futures << Redis::Future.new([:get, PIPELINE_END_SIGNAL], nil, nil)
      end

      redisbetween_enabled ? super[1..-2] : super
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

    def call(command)
      if UNSUPPORTED_COMMANDS.member?(command&.first)
        @handle_unsupported_redisbetween_command&.call(command.first.to_s)
      end
      super
    end
  end

  module RedisPatch
    include Common

    def initialize(options = {})
      redisbetween_options(options)
      super(options)
    end

    def multi(*args, &block)
      @handle_unsupported_redisbetween_command&.call("multi without a block")
      super
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
