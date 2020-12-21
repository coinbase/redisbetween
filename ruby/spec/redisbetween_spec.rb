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
            command=GET args="🔜"
            command=SET args="hi" "1"
            command=GET args="hi"
            command=SET args="yes" "maybe"
            command=GET args="yes"
            command=GET args="🔚"
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
          expect(stream.string).not_to include("🔜")
          expect(stream.string).not_to include("🔚")
        end
      end

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
