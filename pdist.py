import socket
import threading
import json
import time
import signal
import sys
import subprocess
import shlex
import os
from multiprocessing import cpu_count
from cryptography.fernet import Fernet, InvalidToken

try:
    import settings
except ModuleNotFoundError:
    from . import settings

stats = {}
sockets = []

MSGT = {
        'HEARTBEAT' : 1,
        'JOB' : 2,
        'JOB_REQ' : 3
}

if __name__ == '__main__':
    peers = []
    with open(settings.PEERS, "r") as f:
        for p in f.readlines():
            peers.append(p.strip())

if not os.path.isfile(settings.PSK):
    key = Fernet.generate_key()
    with open(settings.PSK, "w") as f:
        f.write(key.decode("utf-8"))
else:
    with open(settings.PSK, "r") as f:
        key = f.read().encode("ascii")

fernet = Fernet(key)

def log(*args, level=0, **kwargs):
    if level < settings.LOG:
        return
    print(*args, **kwargs)

def message(mt, data):
    return [MSGT[mt], data]

def get_mt(mt):
    return [key for key in MSGT if MSGT[key] == mt][0]

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
                log("Unable to connect to", self.target, level=2)
                s.close()
                return False
            self.target = s

        data = fernet.encrypt(str.encode(json.dumps(self.data)))
        ms = len(data).to_bytes(settings.MESSAGE_SIZE, 'big')
        self.target.sendall(ms + data)
        self.target.close()
        return True

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
    try:
        m = get(size)
        data = bytes.decode(fernet.decrypt(m))
        data = json.loads(data)
    except InvalidToken:
        log("Could not decrypt message", level=1)
        return None
    sock.close()
    return data

def hearbeat_handler(data):
    if not stats.get(data['host'], None):
        log("Adding", data['host'], "to peers", level=1)
        stats[data['host']] = {}
    stats[data['host']]['update'] = int(time.time())
    stats[data['host']]['cpu'] = data['cpu']
    stats[data['host']]['mem'] = data['mem']

def job_handler(data):
    logs = open(data['log'], 'w')
    user = data['user']
    cmd = data['cmd']
    cwd = data['cwd']
    cmd = shlex.split("sudo -u {} bash -c {}".format(shlex.quote(user),
        shlex.quote(cmd)))
    subprocess.run(cmd, stdout=logs, stderr=subprocess.STDOUT, cwd=cwd)

def job_request_handler(data):
    host = None
    for h in stats:
        if host == None or stats[h]['cpu'] < stats[host]['cpu']:
            host = h
    log("Distributing job to", host, level=1)
    send(host, message('JOB', data))

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

        if msg is None:
            return

        mt = msg[0]
        data = msg[1]

        if mt == MSGT['HEARTBEAT']:
            hearbeat_handler(data)
        elif mt == MSGT['JOB']:
            job_handler(data)
        elif mt == MSGT['JOB_REQ']:
            job_request_handler(data)

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

class cleaner(threading.Thread):
    def run(self):
        while True:
            time.sleep(settings.CLEAN_INTERVAL)
            t = int(time.time())
            to_pop = []
            for key in stats:
                if t - stats[key]['update'] >= settings.CLEAN_TIMEOUT:
                    to_pop.append(key)
            for p in to_pop:
                log("Removing", p, "from peers", level=1)
                stats.pop(p)

def listen_loop():
    ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssock.bind((socket.gethostname(), settings.PORT))
    ssock.listen()
    sockets.append(ssock)

    log("Server listening on:", ssock.getsockname(), level=1)
    while True:
        csock, addr = ssock.accept()
        log("Incoming connection from", addr)
        st = server(csock, addr)
        st.start()

def client(servers, user, cmd, log, cwd):
    if not type(servers) is list:
        servers = [servers]
    req = {
            'user' : user,
            'cmd' : cmd,
            'log' : log,
            'cwd' : cwd
            }

    for server in servers:
        if send(server, message('JOB_REQ', req), block=True):
            return servers[1:] + servers[:1]
    return None

if __name__ == '__main__':
    def signal_handler(*args, **kwargs):
        for s in sockets:
            s.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    hearbeat().start()
    cleaner().start()
    listen_loop()
