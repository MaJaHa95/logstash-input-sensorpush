# encoding: utf-8
require "logstash/inputs/base"
require "stud/interval"
require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

class SensorPushApiConnection
  def initialize(email, password, logger)
    @session = Net::HTTP.start("api.sensorpush.com", 443, use_ssl: true)
    
    @logger = logger
    @email = email
    @password = password

    @token_hash = {
      expires: 0
    }
  end

  def get_token
    now = Time.now.to_i

    expires = @token_hash[:expires]

    if expires < now
      @logger.debug("Getting SensorPush API authorization token")
      
      auth_req = Net::HTTP::Post.new("/api/v1/oauth/authorize")
      auth_req.body = JSON.generate({
        email: @email,
        password: @password.value
      })
      auth_req["Accept"] = "application/json"
      auth_req["Content-Type"] = "application/json"
      
      auth_resp = @session.request(auth_req)

      auth_resp_json = JSON.parse(auth_resp.body)


      @logger.debug("Getting SensorPush API access token")
      
      token_req = Net::HTTP::Post.new("/api/v1/oauth/accesstoken")
      token_req.body = JSON.generate({
        authorization: auth_resp_json["authorization"]
      })
      token_req["Accept"] = "application/json"
      token_req["Content-Type"] = "application/json"
      
      token_resp = @session.request(token_req)

      token_resp_json = JSON.parse(token_resp.body)

      @token_hash = {
        expires: now + 120,
        response: token_resp_json
      }
    end

    return @token_hash[:response]["accesstoken"]
  end

  def send_request(req)
    req["Accept"] = "application/json"
    req["Authorization"] = self.get_token
    
    resp = @session.request(req)

    resp_json = JSON.parse(resp.body)

    error = resp_json["error_description"]
    if error
      @logger.error("Error received from SensorPush API", :error => error)
    end

    return resp_json
  end

  def get_sensors_map
    resp = self.send_request(Net::HTTP::Post.new("/api/v1/devices/sensors"))
    
    return resp
  end

  def get_latest_values(sensor, to = nil, from = nil)
    sensor_id = sensor["id"]
    
    body = {
      sensors: [sensor_id],
      limit: 300
    }

    if !from.nil?
      body["startTime"] = from.iso8601(3)
    end
    
    if !to.nil?
      body["stopTime"] = to.iso8601(3)
    end
    
    req = Net::HTTP::Post.new("/api/v1/samples")
    req.body = JSON.generate(body)
    resp = self.send_request(req)

    ret = resp["sensors"][sensor_id]

    if ret.nil?
      return []
    end
    
    return ret
  end
end

class LogStash::Inputs::SensorPush < LogStash::Inputs::Base
  config_name "sensorpush"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # The message string to use in the event.
  config :email, :validate => :string
  config :password, :validate => :password
  
  # Set how frequently messages should be sent.
  #
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 10

  public
  def register
    @logger.info("Connecting to SensorPush API", :email => @email)
    @conn = SensorPushApiConnection.new(@email, @password, @logger)
  end # def register

  def run(queue)
    # we can abort the loop if stop? becomes true
    time_by_sensor = {}
    
    while !stop?
      now = DateTime.now
      
      @logger.debug("Getting sensors from SensorPush API")
      sensors = @conn.get_sensors_map

      sensors.each_value { |sensor|
        sensor_id = sensor["id"]

        last_read = time_by_sensor[sensor_id]

        @logger.debug("Getting latest values from SensorPush API", :sensor_id => sensor_id, :since => last_read)

        if last_read
          latest_values = @conn.get_latest_values(sensor, now, last_read)
        else
          latest_values = @conn.get_latest_values(sensor)
        end
  
        unless latest_values.empty?
          time_by_sensor[sensor_id] = DateTime.parse(latest_values.map { |v| v["observed"] }.max)
    
          latest_values.each { |reading|
            timestamp = reading["observed"]
    
            sensorpush = {
              sensor: sensor,
              reading: reading
            }

            event = LogStash::Event.new("@timestamp" => timestamp, "sensorpush" => sensorpush)
            decorate(event)
            queue << event
          }
        end
      }
    
      # because the sleep interval can be big, when shutdown happens
      # we want to be able to abort the sleep
      # Stud.stoppable_sleep will frequently evaluate the given block
      # and abort the sleep(@interval) if the return value is true
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end # def run

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end
end # class LogStash::Inputs::Airthings
