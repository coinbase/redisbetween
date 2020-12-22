require_relative 'lib/redisbetween/version'

Gem::Specification.new do |spec|
  spec.name          = "redisbetween"
  spec.version       = Redisbetween::VERSION
  spec.authors       = ["Jordan Sitkin"]
  spec.email         = ["jordan.sitkin@coinbase.com"]

  spec.summary       = %q{redisbetween client gem}
  spec.description   = %q{Client patch for the ruby redis driver to support redisbetween}
  spec.required_ruby_version = Gem::Requirement.new(">= 2.3.0")

  spec.files = Dir.glob('**')

  spec.require_paths = ["lib"]
  spec.add_dependency "redis"
end
