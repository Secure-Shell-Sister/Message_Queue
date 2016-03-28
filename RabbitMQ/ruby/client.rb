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
result_channel.prefetch 1
result_queue = result_channel.queue('', :exclusive => true)
result_queue.bind(exchange, :routing_key => 'result')
#Handles result from the workey asyncrhonously
result_consumer = result_queue.subscribe(:block => false, :manual_ack => true) do |delivery_info, properties, payload|
	result = JSON.parse payload
	filename = properties.headers['filename']
	print "Got reply #{filename}\n"
	category_hash.merge!(result){|_key, old, new| new + old}

	result_channel.acknowledge(delivery_info.delivery_tag)
	print "Waiting next reply\n"
	result_counter += 1
end

start = Time.now
puts "Sending files"

Dir.glob "#{log_path}/secure*" do |file|
	last_dash = file.rindex(/\//) + 1
	filename = file[last_dash..-1]
	file_counter += 1
	log_content = File.read file
	exchange.publish(log_content, :routing_key => 'log', :headers => {:filename => filename})
end

puts "Waiting for completion"
while file_counter != result_counter do
end

puts "Completed at after #{Time.now - start}"
puts category_hash.sort_by{|_key, value| -value}

result_channel.close
exchange_channel.close
conn.close

