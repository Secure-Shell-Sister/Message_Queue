require 'bunny'
require 'json'

total_worker = 4
host = 'localhost'
mathcer = Regexp.compile '\[\d+\]: (.+?) (for|from)'

total_worker = ARGV[1] unless ARGV[1].nil?
host = ARGV[0] unless ARGV[0].nil?

conn = Bunny.new :host => host
conn.start

channels = []
worker_channel = conn.create_channel nil, total_worker #Set the size for thread pool for consumer
worker_channel.prefetch 1	#One job at a time for every consumer
channels << worker_channel

publish_channel = conn.create_channel
channels << publish_channel

exchange = publish_channel.direct('log_exchanger', :auto_delete => true) #Exchange for sending results and receiving logs

queue = worker_channel.queue('log_parser_queue', :auto_delete => true, :exclusive => false) #Named queue for receiving log files

queue.bind(exchange, :routing_key => 'log')

begin
	puts "Program is running"
	consumer = queue.subscribe(:block => true, :manual_ack => true) do |delivery_info, properties, payload|
		filename = properties.headers['filename']
		print "Got new log #{filename}\n"
		result = Hash.new
		result['Unidentified'] = 0
		payload.lines.each do |line|
			match = mathcer.match line
			key_string = nil
			if match.nil?
				key_string = 'Unidentified'
			elsif match[2] == 'for'
				key_string = match[1]
			else
				space = match[1].rindex ' '
				if space.nil?
					key_string = match[1]
				else
					key_string = match[1][0..(space - 1)]
				end
			end

			if result.include? key_string
				result[key_string] += 1
			else
				result[key_string] = 1
			end
		end

		#Sent back the result
		exchange.publish(result.to_json, :routing_key => 'result', :headers => {:filename => filename})

		worker_channel.acknowledge(delivery_info.delivery_tag)
		print "Done parsing #{filename}\n#{result.to_json}\n"
	end
rescue Interrupt => _
	puts "Closing program"
	#Close all connection when receiving interrupt
	channels.each do |ch|
		ch.close
	end
	conn.close
end
