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

    def process(commands)
      logging(commands) do
        ensure_connected do
          _rb_wrapped_write(commands.size > 1) do
            commands.each do |command|
              if command_map[command.first]
                command = command.dup
                command[0] = command_map[command.first]
              end

              write(command)
            end
          end

          _rb_wrapped_read(commands.size > 1) do
            yield if block_given?
          end
        end
      end
    end

    def _rb_wrapped_write(is_pipeline = false)
      write([:get, PIPELINE_START_SIGNAL]) if is_pipeline && redisbetween_enabled
      yield
      write([:get, PIPELINE_END_SIGNAL]) if is_pipeline && redisbetween_enabled
    end

    # the proxy sends back nil values as placeholders for the signals, so discard them
    def _rb_wrapped_read(is_pipeline = false)
      read if is_pipeline && redisbetween_enabled
      res = yield
      read if is_pipeline && redisbetween_enabled
      res
    end
  end

  module RedisPatch
    include Common

    def initialize(options = {})
      redisbetween_options(options)
      super(options)
    end

    def multi(*args, &block)
      @handle_unsupported_redisbetween_command&.call("multi without a block") unless block_given?
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
