require 'protobuf/rpc/server'
require 'protobuf/logging'
require 'lifeguard'

module Protobuf
  module Rpc
    module Nats
      class Server
        include ::Protobuf::Rpc::Server
        include ::Protobuf::Logging

        def initialize(options)
          @running        = true
          @servers        = options.fetch(:servers, ['nats://10.17.30.94:4222'])
          @subject        = options.fetch(:subject, 'atlas.amigo.>')
          @queue          = options.fetch(:queue, 'atlas.amigo')
          @pool_size      = options.fetch(:threads, 20)
          @pool           = Lifeguard::InfiniteThreadpool.new(:pool_size => @pool_size)
        end

        def log_signature
          @_log_signature ||= "[server-#{self.class.name}]"
        end

        def running?
          @running
        end

        def run
          @unsubscribed = false
          @finished     = false

          NATS.subscribe(@subject, :queue => @queue) do |msg, reply_to, subj|
            @pool.async(msg, reply_to, subj) do |_msg, _reply_to, _subj|
              NATS.publish(_reply_to, handle_request(_msg))
            end
          end

          sleep 1 while running?

          NATS.unsubscribe(@subject, :queue => @queue)
          NATS.flush { @unsubscribed = true }

          sleep 1 until @subscribed
          sleep 1 while @thread_pool.busy_size > 0

          NATS.flush { @finished = true }

          sleep 1 until @finished

          NATS.stop
        end

        def stop
          @running = false
        end
      end
    end
  end
end
