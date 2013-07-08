require 'protobuf/rpc/servers/zmq/util'
require 'protobuf/rpc/servers/zmq/worker'
require 'protobuf/rpc/servers/zmq/broker'
require 'protobuf/rpc/dynamic_discovery.pb'
require 'securerandom'

module Protobuf
  module Rpc
    module Zmq
      class Server
        include ::Protobuf::Rpc::Zmq::Util

        DEFAULT_OPTIONS = {
          :beacon_interval => 5,
          :broadcast_beacons => false
        }

        attr_accessor :options, :workers

        def initialize(options)
          @options = DEFAULT_OPTIONS.merge(options)
          @workers = []

          init_zmq_context
          init_beacon_socket if broadcast_beacons?
          init_shutdown_pipe
        rescue
          teardown
          raise
        end

        def backend_ip
          frontend_ip
        end

        def backend_port
          options[:worker_port] || frontend_port + 1
        end

        def backend_uri
          "tcp://#{backend_ip}:#{backend_port}"
        end

        def beacon_interval
          [options[:beacon_interval].to_i, 1].max
        end

        def beacon_ip
          "255.255.255.255"
        end

        def beacon_port
          unless @beacon_port
            unless port = options[:beacon_port]
              port = ::Protobuf::Rpc::ServiceDirectory.port
            end

            @beacon_port = port.to_i
          end

          @beacon_port
        end

        def beacon_uri
          "udp://#{beacon_ip}:#{beacon_port}"
        end

        def broadcast_beacons?
          !brokerless? && options[:broadcast_beacons]
        end

        def brokerless?
          !!options[:workers_only]
        end

        def frontend_ip
          @frontend_ip ||= resolve_ip(options[:host])
        end

        def frontend_port
          options[:port]
        end

        def frontend_uri
          "tcp://#{frontend_ip}:#{frontend_port}"
        end

        def run
          @running = true

          start_broker unless brokerless?
          start_workers
          wait_for_shutdown_signal
          broadcast_flatline if broadcast_beacons?
        ensure
          teardown
        end

        def running?
          !!@running
        end

        def stop
          @running = false
          @shutdown_w.write('.')
        end

        private

        def broadcast_flatline
          flatline = ::Protobuf::Rpc::DynamicDiscovery::Beacon.new(
            :beacon_type => ::Protobuf::Rpc::DynamicDiscovery::BeaconType::FLATLINE,
            :server => to_proto
          )

          @beacon_socket.send flatline.serialize_to_string, 0
        end

        def broadcast_heartbeat
          @last_beacon = Time.now.to_i

          heartbeat = ::Protobuf::Rpc::DynamicDiscovery::Beacon.new(
            :beacon_type => ::Protobuf::Rpc::DynamicDiscovery::BeaconType::HEARTBEAT,
            :server => to_proto
          )

          @beacon_socket.send(heartbeat.serialize_to_string, 0)

          log_debug { sign_message("sent heartbeat to #{beacon_uri}") }
        end

        def init_beacon_socket
          @beacon_socket = UDPSocket.new
          @beacon_socket.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_BROADCAST, true)
          @beacon_socket.connect(beacon_ip, beacon_port)
        end

        def init_shutdown_pipe
          @shutdown_r, @shutdown_w = IO.pipe
        end

        def init_zmq_context
          @zmq_context = ZMQ::Context.new
        end

        def start_broker
          @broker = Thread.new(self) do |server|
            ::Protobuf::Rpc::Zmq::Broker.new(server).run
          end
        end

        def start_worker
          @workers << Thread.new(self) do |server|
            begin
              ::Protobuf::Rpc::Zmq::Worker.new(server).run
            rescue => e
              msg = "Worker failed: #{e.inspect}\n #{e.backtrace.join($/)}"
              $stderr.puts(msg)
              log_error { msg }

              retry if server.running?
            end
          end
        end

        def start_workers
          this_many = [@options[:threads].to_i, 1].max

          this_many.times { start_worker }

          log_debug { sign_message("#{this_many} workers started") }
        end

        def teardown
          @running = false
          @workers.delete_if(&:join)
          @broker.join unless brokerless?
          @shutdown_r.try(:close)
          @shutdown_w.try(:close)
          @beacon_socket.try(:close)
          @zmq_context.try(:terminate)
        end

        def timeout
          if broadcast_beacons?
            beacon_interval
          else
            nil
          end
        end

        def to_proto
          @proto ||= ::Protobuf::Rpc::DynamicDiscovery::Server.new(
            :uuid => uuid,
            :address => frontend_ip,
            :port => frontend_port.to_s,
            :ttl => (beacon_interval * 1.5).ceil,
            :services => ::Protobuf::Rpc::Service.implemented_services
          )
        end

        def uuid
          @uuid ||= SecureRandom.uuid
        end

        def wait_for_shutdown_signal
          loop do
            break if IO.select([@shutdown_r], nil, nil, timeout)

            broadcast_heartbeat
          end
        end

      end
    end
  end
end
