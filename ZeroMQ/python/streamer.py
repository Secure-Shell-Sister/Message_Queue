#!/usr/bin/env python3

import zmq
from threading import Thread

global sockets
sockets = []

def create_streamer(context, pull_ip, push_ip):

    pusher = context.socket(zmq.PUSH)
    push_bind = "tcp://*:%d" % (push_ip)
    print("Bind pusher on %s" % (push_bind))
    pusher.bind(push_bind)

    puller = context.socket(zmq.PULL)
    pull_bind = "tcp://*:%d" % (pull_ip)
    print("Bind puller on %s" % (pull_bind))
    puller.bind(pull_bind)

    sockets.append(pusher)
    sockets.append(puller)
    zmq.device(zmq.STREAMER, puller, pusher)

if __name__ == "__main__":
    try:
        context = zmq.Context()

        print("Creating first sreamer")
        t = Thread(target=create_streamer, args=(context, 5060, 5061))
        t.start()
        print("Creating second sreamer")
        t2 = Thread(target=create_streamer, args=(context, 5070, 5071))
        t2.start()
        while True:
            pass
    except Exception as e:
        print(e)
        print("Shutdown streamer")
    finally:
        for socket in sockets:
            socket.close()
        context.term()
