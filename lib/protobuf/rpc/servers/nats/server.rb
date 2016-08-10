require 'protobuf/rpc/server'
require 'protobuf/logging'

module Protobuf
  module Rpc
    module Nats
      class Server
        include ::Protobuf::Rpc::Server
        include ::Protobuf::Logging

        def initialize(options)
          @running        = false
          @servers        = options.fetch(:servers)
          @subject        = options.fetch(:subject, 'rpc.>')
          @pool_size      = options.fetch(:threads, 1)
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

          NATS.start(:servers => @servers) do
            NATS.subscribe(@subject, :queue => @queue) do |msg, reply_to, subj|
              if @work_queue.size > @max_queue_size
                NATS.publish(subj, msg, reply_to)
              end

              @thread_pool.async(msg, reply_to, subj) do |_msg, _reply_to, _subj|
                data = handle_request(_msg)
                NATS.publish(_reply_to, data)
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
        end

        def stop
          @running = false
        end
      end
    end
  end
end
