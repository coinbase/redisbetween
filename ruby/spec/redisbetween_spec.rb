RSpec.describe Redisbetween do
  def test_logger(stream, level = Logger::DEBUG)
    logger = Logger.new(stream)
    logger.level = level
    logger.formatter = proc { |_, _, _, msg| msg.sub("[Redis]", "").strip + "\n" }
    logger
  end

  def redis_host
    ENV['REDIS_HOST'] || '127.0.0.1'
  end

  def redis_url(port, db = nil)
    out = "redis://#{redis_host}:#{port}"
    out += "/#{db}" if db
    out
  end

  def redis_socket(port)
    "unix:/var/tmp/redisbetween-#{redis_host}-#{port}.sock"
  end

  def redis_client(cluster:, port:, **opts)
    opts = cluster ? opts.merge(cluster: [redis_url(port)]) : opts.merge(url: redis_url(port))
    Redis.new(opts)
  end

  it "has a version number" do
    expect(Redisbetween::VERSION).not_to be nil
  end
  
  describe '#convert_to_redisbetween_socket' do
    it 'should allow passing a proc' do
      path = Redisbetween.socket_path(->(host, port, path) { [host, port, path].join('-') }, "h", "p", "p")
      expect(path).to eq("h-p-p")
    end

    it 'should default to /var/tmp' do
      path = Redisbetween.socket_path(true, "h", "p", "p")
      expect(path).to eq("/var/tmp/redisbetween-h-p-p.sock")
    end

    it 'should omit the path if not given' do
      path = Redisbetween.socket_path(true, "h", "p")
      expect(path).to eq("/var/tmp/redisbetween-h-p.sock")
    end
  end
  
  describe Redisbetween::ClientPatch do
    it 'should point the client to a socket when given a url' do
      client = Redis.new(url: redis_url(7006), convert_to_redisbetween_socket: true)
      client.get "hi"
      expect(client._client.options[:url]).to eq(redis_socket(7006))
    end

    it 'should point the cluster to a socket when given a url' do
      client = Redis.new(cluster: [redis_url(7000)], convert_to_redisbetween_socket: true)
      client._client.connection_info.map { |l| l[:location] }.each do |loc|
        expect(loc).to match(/\/var\/tmp\/redisbetween-\d+\.\d+\.\d+\.\d+-\d+.sock/)
      end
    end

    it 'should point the client to a socket when given host and port' do
      client = Redis.new(host: redis_host, port: 1234, convert_to_redisbetween_socket: true)
      expect(client._client.options[:url]).to eq(redis_socket(1234))
    end

    it 'should not mess with the url when not enabled' do
      client = Redis.new(url: redis_url(7006, 10))
      expect(client._client.options[:url]).to eq(redis_url(7006, 10))
    end

    [
      [false, 7006],
      [true, 7000],
    ].each do |(cluster, port)|
      context "with cluster #{cluster}, port #{port}" do
        it 'should prepend and append the signal messages to all pipelines when enabled' do
          stream = StringIO.new
          client = redis_client(cluster: cluster, port: port, convert_to_redisbetween_socket: true, logger: test_logger(stream))
          res = client.pipelined do
            client.set("hi", 1)
            client.get("hi")
            client.set("yes", "maybe")
            client.get("yes")
          end
          expect(res).to eq(%w[OK 1 OK maybe])
          expect(stream.string).to include(<<~LOG
            command=SET args="hi" "1"
            command=GET args="hi"
            command=SET args="yes" "maybe"
            command=GET args="yes"
          LOG
          )
        end

        it 'should correctly process transactions with no cross slot keys' do
          stream = StringIO.new
          client = redis_client(cluster: cluster, port: port, convert_to_redisbetween_socket: true, logger: test_logger(stream))
          res = client.multi do
            client.set("{1}hi", 1)
            client.get("{1}hi")
            client.set("{1}yes", "maybe")
            client.get("{1}yes")
          end
          expect(res).to eq(%w[OK 1 OK maybe])
          expect(stream.string).to include(<<~LOG
            command=MULTI args=
            command=SET args="{1}hi" "1"
            command=GET args="{1}hi"
            command=SET args="{1}yes" "maybe"
            command=GET args="{1}yes"
            command=EXEC args=
          LOG
          )
        end

        it 'should not prepend or append the signal messages to any pipelines when not enabled' do
          stream = StringIO.new
          client = redis_client(cluster: cluster, port: port, logger: test_logger(stream))
          res = client.pipelined do
            client.set("hi", 1)
            client.get("hi")
            client.set("yes", "maybe")
            client.get("yes")
          end
          expect(res).to eq(%w[OK 1 OK maybe])
          expect(stream.string).to include(<<~LOG
            command=SET args="hi" "1"
            command=GET args="hi"
            command=SET args="yes" "maybe"
            command=GET args="yes"
          LOG
          )
        end

        it 'should not prepend or append the signal messages to multis when not enabled' do
          stream = StringIO.new
          client = redis_client(cluster: cluster, port: port, logger: test_logger(stream))
          res = client.multi do
            client.set("{1}hi", 1)
            client.get("{1}hi")
            client.set("{1}yes", "maybe")
            client.get("{1}yes")
          end
          expect(res).to eq(%w[OK 1 OK maybe])
          expect(stream.string).to include(<<~LOG
            command=MULTI args=
            command=SET args="{1}hi" "1"
            command=GET args="{1}hi"
            command=SET args="{1}yes" "maybe"
            command=GET args="{1}yes"
            command=EXEC args=
          LOG
          )
        end
      end
    end

    describe :handle_unsupported_redisbetween_command do
      it 'should raise on unsupported commands when set to :raise' do
        client = Redis.new(
          url: redis_url(7006, 10),
          handle_unsupported_redisbetween_command: ->(_cmd) { raise "hi" }
        )
        expect { client.select(2) }.to raise_error("hi")
      end

      it 'standalone: should call the given proc' do
        stream = StringIO.new
        logger = test_logger(stream)
        client = Redis.new(
          url: redis_url(7006, 10),
          handle_unsupported_redisbetween_command: ->(cmd) { logger.warn("hi #{cmd}") }
        )
        client.select(2)
        expect(stream.string).to include("hi select")
      end

      it 'cluster: should call the given proc' do
        stream = StringIO.new
        logger = test_logger(stream)
        client = Redis.new(
          cluster: [redis_url(7000)],
          handle_unsupported_redisbetween_command: ->(cmd) { logger.warn(cmd) }
        )
        client.wait(1, 1)
        expect(stream.string).to include("wait")
      end

      it 'should not mess with unsupported commands when not enabled' do
        client = Redis.new(url: redis_url(7006, 10))
        expect(client.select(2)).to eq("OK")
      end

      it 'should disallow multi without a block' do
        client = Redis.new(
          url: redis_url(7006, 10),
          handle_unsupported_redisbetween_command: ->(cmd) { raise cmd }
        )
        expect { client.multi }.to raise_error("multi without a block")
      end
    end
  end
end
