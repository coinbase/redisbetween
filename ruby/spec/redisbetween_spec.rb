RSpec.describe Redisbetween do
  def test_logger(stream)
    logger = Logger.new(stream)
    logger.level = Logger::DEBUG
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
      expected = [
        "/var/tmp/redisbetween-127.0.0.1-7000.sock",
        "/var/tmp/redisbetween-127.0.0.1-7001.sock",
        "/var/tmp/redisbetween-127.0.0.1-7002.sock",
      ]
      expect(client._client.connection_info.map { |l| l[:location] }.sort).to eq(expected)
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
    it 'should prepend and append the signal messages to all pipelines when enabled' do
      stream = StringIO.new
      client = Redis.new(url: 'redis://127.0.0.1:7006/1', redisbetween_pipeline_signals_enabled: true, logger: test_logger(stream))
      client.pipelined do
        client.set("hi", 1)
        client.get("hi")
      end
      expect(stream.string).to start_with(<<~LOG
        command=GET args="🔜"
        command=SET args="hi" "1"
        command=GET args="hi"
        command=GET args="🔚"
      LOG
      )
    end

    it 'should not prepend or append the signal messages to any pipelines when not enabled' do
      stream = StringIO.new
      client = Redis.new(url: 'redis://127.0.0.1:7006/1', logger: test_logger(stream))
      client.pipelined do
        client.set("hi", 1)
        client.get("hi")
      end
      expect(stream.string).to start_with(<<~LOG
        command=SET args="hi" "1"
        command=GET args="hi"
      LOG
      )
    end

    it 'should disallow unsupported commands when enabled' do
      client = Redis.new(url: 'redis://127.0.0.1:7006/10', disallow_unsupported_redisbetween_commands: true)
      expect { client.select(2) }.to raise_error(Redisbetween::Error)
    end

    it 'should not mess with unsupported commands when not enabled' do
      client = Redis.new(url: 'redis://127.0.0.1:7006/10')
      expect(client.select(2)).to eq("OK")
    end

    it 'should disallow multi without a block' do
      client = Redis.new(url: 'redis://127.0.0.1:7006/10', disallow_unsupported_redisbetween_commands: true)
      expect { client.multi }.to raise_error(Redisbetween::Error)
    end
  end
end
