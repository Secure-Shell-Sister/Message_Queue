#!/usr/bin/env python3

import zmq
import sys
import os

def send_file(path, context):
    file_counter = 0
    for file_name in os.listdir(path):
        if file_name.startswith("secure"):
            file_counter += 1
            logfile = open("%s/%s" % (path, file_name))
            context.send_string(logfile.read())
            logfile.close()

    return file_counter

def receive_result(context, file_counter):
    data_collector = {}
    results = []
    for i in range(file_counter):
        result = context.recv_json()
        results.append(result)

    for result in results:
        for key, value in result.items():
            if key in data_collector:
                data_collector[key] += value
            else:
                data_collector[key] = value

    return data_collector


def main(log_path, host):
    print("Creating context")
    context = zmq.Context()
    push_socket = context.socket(zmq.PUSH)
    pull_socket = context.socket(zmq.PULL)

    print("Binding sockets")
    push_socket.connect("tcp://%s:5060" % (host))
    pull_socket.connect("tcp://%s:5071" % (host))

    print("Sending files")
    total_file = send_file(log_path, push_socket)
    if total_file <= 0:
        print("The path don't contain any specified log")
        return 0
    print("%d file(s) sent" % (total_file))

    print("Waiting for reply")
    final_result = receive_result(pull_socket, total_file)
    if final_result == {}:
        print("Log parsing doesn't run according to plan")
        return 0

    sorted_result = sorted(final_result, key=final_result.__getitem__)
    for key in sorted_result:
        print("%s: %s" % (key, final_result[key]))

    push_socket.close()
    pull_socket.close()
    context.term()

if __name__ == "__main__":
    log_path = "/home/revant/Documents/SISTER/RPC/NewRPC/var/log"
    host = "127.0.0.1"
    if len(sys.argv) > 1:
        log_path = sys.argv[1]
    if len(sys.argv) > 2:
        host = sys.argv[2]
    main(log_path, host)
