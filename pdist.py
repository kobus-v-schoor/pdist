import socket
import threading
import settings
import pickle
import time
import signal
import sys
import subprocess
import shlex
import os
from multiprocessing import cpu_count
from cryptography.fernet import Fernet, InvalidToken

stats = {}
sockets = []
def signal_handler(*args, **kwargs):
    for s in sockets:
        s.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

MSGT = {
        'HEARTBEAT' : 1,
        'JOB' : 2,
        'JOB_REQ' : 3
}

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
                log("Unable to connect to", self.target)
                s.close()
                return
            self.target = s

        data = fernet.encrypt(pickle.dumps(self.data))
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
    try:
        m = get(size)
        data = pickle.loads(fernet.decrypt(m))
    except InvalidToken:
        log("Could not decrypt message")
        return None
    sock.close()
    return data

def hearbeat_handler(data):
    if not stats.get(data['host'], None):
        log("Adding", data['host'], "to peers")
        stats[data['host']] = {}
    stats[data['host']]['update'] = int(time.time())
    stats[data['host']]['cpu'] = data['cpu']
    stats[data['host']]['mem'] = data['mem']

def job_handler(data):
    logs = open(data['log'], 'w')
    user = data['user']
    cmd = data['cmd']
    cwd = data['cwd']
    cmd = shlex.split("sudo -u {} {}".format(shlex.quote(user), shlex.quote(cmd)))
    subprocess.run(cmd, stdout=logs, stderr=subprocess.STDOUT, cwd=cwd)

def job_request_handler(data):
    host = None
    for h in stats:
        if host == None or stats[h]['cpu'] < stats[host]['cpu']:
            host = h
    log("Distributing job to", host)
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

        if msg.mt == MSGT['HEARTBEAT']:
            hearbeat_handler(msg.data)
        elif msg.mt == MSGT['JOB']:
            job_handler(msg.data)
        elif msg.mt == MSGT['JOB_REQ']:
            job_request_handler(msg.data)

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
                log("Removing", p, "from peers")
                stats.pop(p)

def listen_loop():
    ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssock.bind((socket.gethostname(), settings.PORT))
    ssock.listen()
    sockets.append(ssock)

    log("Server listening on:", ssock.getsockname())
    while True:
        csock, addr = ssock.accept()
        log("Incoming connection from", addr)
        st = server(csock, addr)
        st.start()

def client(user, cmd, log, cwd):
    req = {
            'user' : user,
            'cmd' : cmd,
            'log' : log,
            'cwd' : cwd
            }

    send(settings.SERVER, message('JOB_REQ', req), block=True)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Please specify \"server\" or \"client\"")
        sys.exit(1)

    if sys.argv[1] == "server":
        hearbeat().start()
        cleaner().start()
        listen_loop()
    elif sys.argv[1] == "client":
        if len(sys.argv) != 6:
            print("Please specify the user to run the command as,",
                    "the command and the log file location")
            sys.exit(1)

        cmd = {}
        cmd['user'] = sys.argv[2]
        cmd['cmd'] = sys.argv[3]
        cmd['log'] = sys.argv[4]
        cmd['cwd'] = sys.argv[5]

        client(**cmd)
    else:
        print("Invalid argument, needs to be either \"server\" or \"client\"")
        sys.exit(1)
