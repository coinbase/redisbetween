RSpec.describe Redisbetween do
  def test_logger(stream, level = Logger::DEBUG)
    logger = Logger.new(stream)
    logger.level = level
    logger.formatter = proc { |_, _, _, msg| msg.sub("[Redis]", "").strip + "\n" }
    logger
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
      client = Redis.new(url: "redis://127.0.0.1:7006", convert_to_redisbetween_socket: true)
      client.get "hi"
      expect(client._client.options[:url]).to eq("unix:/var/tmp/redisbetween-127.0.0.1-7006.sock")
    end

    it 'should point the cluster to a socket when given a url' do
      client = Redis.new(cluster: ["redis://127.0.0.1:7000"], convert_to_redisbetween_socket: true)
      client._client.connection_info.map { |l| l[:location] }.each do |loc|
        expect(loc).to match(/\/var\/tmp\/redisbetween-\d+\.\d+\.\d+\.\d+-\d+.sock/)
      end
    end

    it 'should point the client to a socket when given host and port' do
      client = Redis.new(host: '127.0.0.1', port: 1234, convert_to_redisbetween_socket: true)
      expect(client._client.options[:url]).to eq("unix:/var/tmp/redisbetween-127.0.0.1-1234.sock")
    end

    it 'should not mess with the url when not enabled' do
      client = Redis.new(url: 'redis://127.0.0.1:7006/10')
      expect(client._client.options[:url]).to eq('redis://127.0.0.1:7006/10')
    end
  end

  describe Redisbetween::RedisPatch do
    [
      { url: 'redis://127.0.0.1:7006' }, # standalone
      { cluster: ['redis://127.0.0.1:7000'] }, # cluster
    ].each do |options|
      context "with #{options}" do
        it 'should prepend and append the signal messages to all pipelines when enabled' do
          stream = StringIO.new
          client = Redis.new(options.merge({ convert_to_redisbetween_socket: true, logger: test_logger(stream) }))
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
          client = Redis.new(options.merge({ convert_to_redisbetween_socket: true, logger: test_logger(stream) }))
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
          client = Redis.new(options.merge({ logger: test_logger(stream) }))
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
          client = Redis.new(options.merge({ logger: test_logger(stream) }))
          res = client.multi do
            client.set("{1}hi", 1)
            client.get("{1}hi")
            client.set("{1}yes", "maybe")
            client.get("{1}yes")
          end
          expect(res).to eq(%w[OK 1 OK maybe])
          expect(stream.string).to include(<<~LOG
            command=SET args="{1}hi" "1"
            command=GET args="{1}hi"
            command=SET args="{1}yes" "maybe"
            command=GET args="{1}yes"
          LOG
          )
        end
      end
    end

    describe :handle_unsupported_redisbetween_commands do
      it 'should raise on unsupported commands when set to :raise' do
        client = Redis.new(
          url: 'redis://127.0.0.1:7006/10',
          handle_unsupported_redisbetween_commands: ->(_cmd) { raise "hi" }
        )
        expect { client.select(2) }.to raise_error("hi")
      end

      it 'standalone: should call the given proc' do
        stream = StringIO.new
        logger = test_logger(stream)
        client = Redis.new(
          url: 'redis://127.0.0.1:7006/10',
          handle_unsupported_redisbetween_commands: ->(cmd) { logger.warn("hi #{cmd}") }
        )
        client.select(2)
        expect(stream.string).to include("hi select")
      end

      it 'cluster: should call the given proc' do
        stream = StringIO.new
        logger = test_logger(stream)
        client = Redis.new(
          cluster: ['redis://127.0.0.1:7000'],
          handle_unsupported_redisbetween_commands: ->(cmd) { logger.warn(cmd) }
        )
        client.wait(1, 1)
        expect(stream.string).to include("wait")
      end

      it 'should not mess with unsupported commands when not enabled' do
        client = Redis.new(url: 'redis://127.0.0.1:7006/10')
        expect(client.select(2)).to eq("OK")
      end

      it 'should disallow multi without a block' do
        client = Redis.new(
          url: 'redis://127.0.0.1:7006/10',
          handle_unsupported_redisbetween_commands: ->(cmd) { raise cmd }
        )
        expect { client.multi }.to raise_error("multi without a block")
      end
    end
  end
end
