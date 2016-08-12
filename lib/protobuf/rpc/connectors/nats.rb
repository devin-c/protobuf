require 'protobuf/rpc/connectors/base'

module Protobuf
  module Rpc
    module Connectors
      class Nats < Base
        include Protobuf::Rpc::Connectors::Common
        include Protobuf::Logging

        def send_request
          3.times do
            queue = Queue.new
            subject = options[:service].to_s.underscore.gsub('/', '.') + ".#{options[:method]}"
            sid = NATS.request(subject, @request_data, :max => 1) do |resp|
              queue.push(resp)
            end

            NATS.timeout(sid, 10) { queue.push :timeout }

            resp = queue.pop

            if resp != :timeout
              @response_data = resp
              return
            end
          end

          @error = true
        end

        def close_connection; end

        def log_signature
          @_log_signature ||= "[client-#{self.class}]"
        end

        private
        # Method to determine error state, must be used with Connector api
        def error?
          return true if @error
          logger.debug { sign_message("Error") }
        end
      end
    end
  end
end
