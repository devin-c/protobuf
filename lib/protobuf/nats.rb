##
## NATS Mode
##
#
# Require this file if you wish to run your server and/or client RPC
# with the NATS handlers.
#
# To run with rpc_server specify the switch `nats`:
#
#   rpc_server --nats myapp.rb
#
# To run for client-side only override the require in your Gemfile:
#
#   gem 'protobuf', :require => 'protobuf/nats'
#
require 'protobuf'
Protobuf.connector_type = :nats

require 'nats/client'
require 'protobuf/rpc/servers/nats/server'
require 'protobuf/rpc/connectors/nats'
