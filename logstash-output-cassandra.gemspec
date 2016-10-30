Gem::Specification.new do |s|

  s.name            = 'logstash-output-cassandra'
  s.version         = '5.0.0'
  s.licenses        = [ 'Apache License (2.0)' ]
  s.summary         = 'Store events into Cassandra'
  s.description     = 'This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program'
  s.authors         = [ 'PerimeterX' ]
  s.email           = [ 'elad@perimeterx.com' ]
  s.homepage        = 'https://github.com/PerimeterX/logstash-output-cassandra'
  s.require_paths   = [ 'lib' ]

  # Files
  s.files = Dir[ 'lib/**/*', 'spec/**/*', 'vendor/**/*', '*.gemspec', '*.md', 'CONTRIBUTORS', 'Gemfile', 'LICENSE', 'NOTICE.TXT' ]
  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { 'logstash_plugin' => 'true', 'logstash_group' => 'output' }

  # Gem dependencies
  s.add_runtime_dependency 'concurrent-ruby'
  s.add_runtime_dependency 'logstash-core-plugin-api', '>= 1.60', '<= 2.99'
  s.add_runtime_dependency 'cassandra-driver', '>= 3.0.0'
  s.add_development_dependency 'cabin'
  s.add_development_dependency 'longshoreman'
  s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency 'logstash-codec-plain'
  s.add_development_dependency 'simplecov'
  s.add_development_dependency 'simplecov-rcov'
  s.add_development_dependency 'gems'
end
