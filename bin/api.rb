#  Copyright 2020-2021 Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

require "openssl"
require "net/http"
require "json"

class Object
  def to_b
    ![nil, false, 0, "", "0", "f", "F", "false", "FALSE", "off", "OFF", "no", "NO"].include?(self)
  end
end


class API
  def initialize(options)
    @options = options
    connect
  end

  def connect
    puts "# CONNECT #{@options}"
    @client = Net::HTTP.start(@options[:host], @options[:port],
                              use_ssl: @options[:strict_encryption],
                              verify_mode: OpenSSL::SSL::VERIFY_NONE)
  end

  def url(path)
    "#{@client.use_ssl? ? 'https' : 'http'}://#{@options[:username]}@#{@options[:host]}:#{@options[:port]}#{path}"
  end

  def decode_response(response)
    payload =
      if response['content-type'] =~ /application\/json/
        JSON.parse(response.body)
      else
        response.body
      end
    if @options[:verbose]
      p status: response.code, payload: payload
    end
    payload
  end

  def setup_request(request)
    request.basic_auth(@options[:username], @options[:password])
    request['accept'] = "application/json"
  end

  def get(path)
    puts "# GET #{url(path)}"
    req = Net::HTTP::Get.new(path)
    setup_request(req)
    res = @client.request(req)
    decode_response(res)
  rescue EOFError
    connect
    retry
  rescue => ex
    puts "#{__method__}: #{ex}, sleep for 1 second and retry"
    sleep(1)
    retry
  end

  def post_form(path, fields = {})
    puts "# POST #{url(path)} #{fields}"
    req = Net::HTTP::Post.new(path)
    setup_request(req)
    req.form_data = fields
    res = @client.request(req)
    decode_response(res)
  rescue EOFError
    connect
    retry
  rescue => ex
    puts "#{__method__}: #{ex}, sleep for 1 second and retry"
    sleep(1)
    retry
  end

  def post_json(path, object)
    data = JSON.generate(object)
    puts "# POST #{url(path)} #{data}"
    req = Net::HTTP::Post.new(path)
    req['content-type'] = "application/json"
    setup_request(req)
    res = @client.request(req, data)
    decode_response(res)
  rescue EOFError
    connect
    retry
  rescue => ex
    puts "#{__method__}: #{ex}, sleep for 1 second and retry"
    sleep(1)
    retry
  end

  def put_json(path, object)
    data = JSON.generate(object)
    puts "# PUT #{url(path)} #{data}"
    req = Net::HTTP::Put.new(path)
    req['content-type'] = "application/json"
    setup_request(req)
    res = @client.request(req, data)
    decode_response(res)
  rescue EOFError
    connect
    retry
  rescue => ex
    puts "#{__method__}: #{ex}, sleep for 1 second and retry"
    sleep(1)
    retry
  end

  def delete(path)
    puts "# DELETE #{url(path)}"
    req = Net::HTTP::Delete.new(path)
    setup_request(req)
    res = @client.request(req)
    decode_response(res)
  rescue EOFError
    connect
    retry
  rescue => ex
    puts "#{__method__}: #{ex}, sleep for 1 second and retry"
    sleep(1)
    retry
  end
end

def service_address_for_bucket(api, service, bucket, options)
  port_key = options[:strict_encryption] ? "#{service}SSL" : service
  host = nil
  port = 0
  while port.zero?
    config = api.get("/pools/default/b/#{bucket}")
    node_with_service = config["nodesExt"].find{|n| n["services"].key?(port_key)} rescue nil
    if node_with_service
      host = node_with_service["hostname"]
      port = node_with_service["services"][port_key].to_i rescue 0
    end
    sleep 1
  end
 {host: host || options[:host], port: port}
end

def ensure_search_index_created(index_definition_path, options)
  loop do
    catch :retry do
      search_api = API.new(options)

      puts "using index definition: #{File.basename(index_definition_path)}"
      index_definition = JSON.load(File.read(index_definition_path))
      search_api.put_json("/api/index/travel-sample-index", index_definition)

      expected_number_of_the_documents = 1000
      indexed_documents = 0
      times_zero_documents_has_been_seen = 0
      Timeout.timeout(10 * 60) do # give it 10 minutes to index
        while indexed_documents < expected_number_of_the_documents
          resp = search_api.get("/api/index/travel-sample-index/count")
          indexed_documents = resp['count'].to_i
          times_zero_documents_has_been_seen += 1 if indexed_documents.zero?
          if times_zero_documents_has_been_seen >= 60
            # lets not tolerate 60 seconds of not getting data indexed
            search_api.delete("/api/index/travel-sample-index")
            throw :retry
          end
          sleep 1
        end
      end
      return
    end
  end
end

def ensure_sample_bucket_loaded(management_api, expected_number_of_the_documents, options)
  loop do
    catch :retry do
      management_api.post_json("/sampleBuckets/install", [options[:bucket]])

      service_address = nil
      begin
        Timeout.timeout(60) do # a minute for bucket creation
          service_address = service_address_for_bucket(management_api, "n1ql", options[:bucket], options)
        end
      rescue Timeout::Error
        management_api.delete("/pools/default/buckets/#{options[:bucket]}")
        sleep 1
        throw :retry
      end
      puts query_service: service_address

      query_api = API.new(options.merge(service_address))
      puts "create index for bucket \"#{options[:bucket]}\""

      query_api.post_form("/query/service", statement: "CREATE PRIMARY INDEX ON `#{options[:bucket]}` USING GSI", timeout: "300s")

      indexed_documents = 0
      times_zero_documents_has_been_seen = 0
      Timeout.timeout(10 * 60) do # give it 10 minutes to index
        while indexed_documents < expected_number_of_the_documents
          resp = query_api.post_form("/query/service", statement: "SELECT RAW COUNT(*) FROM `#{options[:bucket]}`")
          indexed_documents = resp["results"][0].to_i rescue 0

          if indexed_documents.zero?
            times_zero_documents_has_been_seen += 1
            puts times_zero_documents_has_been_seen: times_zero_documents_has_been_seen
          end
          if times_zero_documents_has_been_seen >= 60
            # lets not tolerate 60 seconds of not getting data indexed
            query_api.post_form("/query/service", statement: "DROP PRIMARY INDEX ON `#{options[:bucket]}`")
            management_api.delete("/pools/default/buckets/#{options[:bucket]}")
            sleep 1
            throw :retry
          end
          sleep 1
        end
      end
      return
    end
  end
end
