# Used as component of a rubyrep config file.
# Defines connection parameters to the postgresql databases.

RR::Initializer::run do |config|
  config.left = {
    :adapter  => 'postgresql',   
    :database => 'rr_left',
    :username => 'rubyrep',   
    :password => 'password',   
    :host     => 'pg',
    :min_messages => 'warning'
  }

  config.right = {
    :adapter  => 'postgresql',   
    :database => 'rr_right',   
    :username => 'rubyrep',   
    :password => 'password',   
    :host     => 'pg',
    :min_messages => 'warning'
  }

end