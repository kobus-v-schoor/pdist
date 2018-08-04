#! /usr/bin/python

import socket
import threading
import settings
import pickle
import time
import signal
import sys
import subprocess
from multiprocessing import cpu_count

sockets = []
def signal_handler(*args, **kwargs):
    for s in sockets:
        s.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

MSGT = {
        'HEARTBEAT' : 1
}

peers = []
with open(settings.PEERS, "r") as f:
    for p in f.readlines():
        peers.append(p.strip())

def log(*args, **kwargs):
    print(*args, **kwargs)

class message:
    def __init__(self, mt, data):
        self.mt = MSGT[mt]
        self.data = data

    def get_mt(self):
        return [key for key in MSGT if MSGT[key] == self.mt][0]

class send(threading.Thread):
    def __init__(self, target, data, block=False, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.target = target
        self.data = data
        if block:
            self.run()
        else:
            self.start()

    def run(self):
        if type(self.target) is str:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((self.target, settings.PORT))
            except ConnectionRefusedError:
                s.close()
                return
            self.target = s

        data = pickle.dumps(self.data)
        ms = len(data).to_bytes(settings.MESSAGE_SIZE, 'big')
        self.target.sendall(ms + data)
        self.target.close()

def recv(sock):
    def get(size):
        cp = 0
        chunks = bytearray()
        while cp < size:
            chunk = sock.recv(min(settings.RECEIVE_BUFF, size))
            if not chunk:
                break
            cp += len(chunk)
            chunks += chunk
        if not chunks:
            raise RuntimeError('Connection error')
        return bytes(chunks)

    size = int.from_bytes(get(settings.MESSAGE_SIZE), 'big')
    data = pickle.loads(get(size))
    sock.close()
    return data

def hearbeat_handler(data):
    pass

class server(threading.Thread):
    def __init__(self, csock, addr, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.csock = csock
        self.addr = addr

    def pre(self):
        return "[{}]".format(addr[0])

    def log(self, *args, **kwargs):
        log(pre(), *args, **kwargs)

    def run(self):
        msg = recv(self.csock)

        if msg.mt == MSGT['HEARTBEAT']:
            hearbeat_handler(msg.data)

def collect_stats():
    def cpu_load():
        s = subprocess.check_output(["uptime"]).decode("utf-8").strip()
        s = [n.strip() for n in s.split()][-3:][1][:-1] # Use 5-minute average load
        return float(s) / cpu_count()

    def free_mem():
        s = subprocess.check_output(["free"]).decode("utf-8").split("\n")[1]
        s = int(s.split()[-1])
        return s

    stat = {}

    stat['host'] = socket.gethostname()
    stat['cpu'] = cpu_load()
    stat['mem'] = free_mem()

    return stat

class hearbeat(threading.Thread):
    def run(self):
        while True:
            time.sleep(settings.HEARTBEAT)
            for p in peers:
                send(p, message('HEARTBEAT', collect_stats()))

def listen_loop():
    ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssock.bind((socket.gethostname(), settings.PORT))
    ssock.listen()
    sockets.append(ssock)

    log("Server listening on:", ssock.getsockname())
    while True:
        csock, addr = ssock.accept()
        log("Client connected from", addr)
        st = server(csock, addr)
        st.start()

hearbeat().start()
listen_loop()
