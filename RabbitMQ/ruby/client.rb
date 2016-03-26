require 'bunny'
require 'json'

host = 'localhost'
log_path = "/home/revant/Documents/SISTER/RPC/NewRPC/var/log"

log_path = ARGV[0] unless ARGV[0].nil?
host = ARGV[1] unless ARGV[1].nil?

conn = Bunny.new :host => host
conn.start

result_counter = 0
file_counter = 0
category_hash = Hash.new

exchange_channel = conn.create_channel
exchange = exchange_channel.direct('log_exchanger', :auto_delete => true)

result_channel = conn.create_channel
result_queue = result_channel.queue('', :exclusive => true)
result_queue.bind(exchange, :routing_key => 'result')
#Handles result from the workey asyncrhonously
result_consumer = result_queue.subscribe(:block => false) do |deivery_info, properties, payload|
	result = JSON.parse payload
	result.each do |key, value|
		if category_hash.include? key
			category_hash[key] += value
		else
			category_hash[key] = value
		end
	end
	result_counter += 1
end

Dir.glob "#{log_path}/secure*" do |file|
	file_counter += 1
	log_content = File.read file
	exchange.publish(log_content, :routing_key => 'log', :headers => {:filename => file})
end

while file_counter != result_counter do
end

puts category_hash

result_channel.close
exchange_channel.close
conn.close

