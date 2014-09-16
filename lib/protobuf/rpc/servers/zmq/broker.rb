require 'thread'

module Protobuf
  module Rpc
    module Zmq
      class Broker
        include ::Protobuf::Rpc::Zmq::Util

        attr_reader :job_queue

        def initialize(server)
          @server = server

          init_zmq_context
          init_backend_socket
          init_frontend_socket
          init_poller
        rescue
          teardown
          raise
        end

        def job_queue
          @server.job_queue
        end

        def run
          loop do
            rc = @poller.poll(500)

            # No responses were pending, the server was shutdown, and all
            # workers have finished and been reaped
            break if rc == 0 && !running? && workers.empty?

            # Something went wrong
            break if rc == -1

            @poller.readables.each do |readable|
              case readable
              when @frontend_socket
                process_frontend
              when @backend_socket
                process_backend
              end
            end
          end
        ensure
          teardown
        end

        def running?
          @server.running?
        end

        def workers
          @server.workers
        end

        private

        def init_backend_socket
          @backend_socket = @zmq_context.socket(ZMQ::ROUTER)
          zmq_error_check(@backend_socket.bind(@server.backend_uri))
        end

        def init_frontend_socket
          @frontend_socket = @zmq_context.socket(ZMQ::ROUTER)
          zmq_error_check(@frontend_socket.bind(@server.frontend_uri))
        end

        def init_poller
          @poller = ZMQ::Poller.new
          @poller.register_readable(@frontend_socket)
          @poller.register_readable(@backend_socket)
        end

        def init_zmq_context
          if inproc?
            @zmq_context = @server.zmq_context
          else
            @zmq_context = ZMQ::Context.new
          end
        end

        def inproc?
          !!@server.try(:inproc?)
        end

        def job_queue_max_size
          @job_queue_max_size ||= [ENV["PB_ZMQ_SERVER_QUEUE_MAX_SIZE"].to_i, 5].max
        end

        def process_backend
          worker, ignore, *frames = read_from_backend

          unless frames == [::Protobuf::Rpc::Zmq::WORKER_READY_MESSAGE]
            write_to_frontend(frames)
          end
        end

        def process_frontend
          address, _, message, *frames = read_from_frontend

          if message == ::Protobuf::Rpc::Zmq::CHECK_AVAILABLE_MESSAGE
            if job_queue.size < job_queue_max_size && running?
              write_to_frontend([address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, ::Protobuf::Rpc::Zmq::WORKERS_AVAILABLE])
            else
              write_to_frontend([address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, ::Protobuf::Rpc::Zmq::NO_WORKERS_AVAILABLE])
            end
          else
            job_queue << [address, ::Protobuf::Rpc::Zmq::EMPTY_STRING, message ].concat(frames)
          end
        end

        def read_from_backend
          frames = []
          zmq_error_check(@backend_socket.recv_strings(frames))
          frames
        end

        def read_from_frontend
          frames = []
          zmq_error_check(@frontend_socket.recv_strings(frames))
          frames
        end

        def teardown
          @frontend_socket.try(:close)
          @backend_socket.try(:close)
          @zmq_context.try(:terminate) unless inproc?
        end

        def write_to_backend(frames)
          zmq_error_check(@backend_socket.send_strings(frames))
        end

        def write_to_frontend(frames)
          zmq_error_check(@frontend_socket.send_strings(frames))
        end
      end
    end
  end
end
