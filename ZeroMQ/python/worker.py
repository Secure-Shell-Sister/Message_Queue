#!/usr/bin/env python3

import zmq
import re
import sys

def parse_log(sender, receiver):
    matcher = re.compile('\[\d+\]: (.+?) (for|from)')
    while True:
        content = receiver.recv_string()
        print("Got a job")
        category_counter = dict()
        matches = matcher.finditer(content)
        file_length = len(content.splitlines())
        matches_length = 0
        for match in matches:
            matches_length += 1
            key_string = ''
            if match.group(2) == 'for':
                key_string = match.group(1)
            else:
                separated_string = match.group(1).split(' ')
                if len(separated_string) > 1:
                    key_string = (' ').join(separated_string[:-1])
                else:
                    key_string = separated_string[0]

            if key_string in category_counter:
                category_counter[key_string] += 1
            else:
                category_counter[key_string] = 1

        category_counter["Unidentified"] = file_length - matches_length
        sender.send_json(category_counter)


if __name__ == "__main__":
    host = "127.0.0.1"
    if len(sys.argv) > 1:
        host = sys.argv[1]

    context = zmq.Context()
    consumer_receiver = context.socket(zmq.PULL)
    consumer_receiver.connect("tcp://%s:5061" % (host))

    consumer_sender = context.socket(zmq.PUSH)
    consumer_sender.connect("tcp://%s:5070" % (host))

    print("Starting the worker")
    parse_log(consumer_sender, consumer_receiver)
