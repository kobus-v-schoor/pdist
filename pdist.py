import socket
import threading
import json
import time
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

MSGT = {
        'HEARTBEAT' : 1, # Nodes use to notify peers of load values
        'JOB' : 2, # Starts job on node
        'JOB_REQ' : 3, # Requests job to be distributed
        'JOB_ID' : 4, # Node notifies client of id info
        'JOB_EXIT' : 5, # Node notifies client that job is done
        'JOB_TERM' : 6, # Client notifies node to terminate job
        'JOB_RES' : 7, # Client notifies node that it wants to resume job
        'JOB_RES_ACC' : 8, # Node notifies client that job is still running
        'JOB_DNE' : 9, # Node notifies client that job doesn't exist (exited)
}

if __name__ == '__main__':
    peers = []
    with open(settings.PEERS, "r") as f:
        for p in f.readlines():
            peers.append(p.strip())

    jobs = {}
    job_lock = threading.Lock()

if not os.path.isfile(settings.PSK):
    key = Fernet.generate_key()
    with open(settings.PSK, "w") as f:
        f.write(key.decode())
else:
    with open(settings.PSK, "r") as f:
        key = f.read().encode()

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
                t = self.target.split(":")
                a = t[0]
                if len(t) == 2:
                    p = int(t[1])
                else:
                    p = settings.PORT
                s.connect((a, p))
            except ConnectionRefusedError:
                log("Unable to connect to", self.target)
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

# Abstraction
def get_job_id():
    get_job_id.cur += 1
    return get_job_id.cur
get_job_id.cur = -1

def job_handler(data):
    logs = open(data['log'], 'w')
    user = data['user']
    cmd = data['cmd']
    cwd = data['cwd']
    cmd = shlex.split("su {} -c {}".format(shlex.quote(user), shlex.quote(cmd)))

    proc = subprocess.Popen(cmd, stdout=logs, stderr=subprocess.STDOUT, cwd=cwd)

    job_lock.acquire()
    jid = get_job_id()

    job = {
            'proc': proc,
            'id': jid,
            'addr': [data['addr']],
            'port': [data['port']]
            }

    jobs[jid] = job
    job_lock.release()

    log("Starting job ID", jid, level=1)

    id_msg = {
            'id' : jid,
            'node' : "{}:{}".format(socket.gethostname(), settings.PORT)
            }

    send("{}:{}".format(data['addr'], data['port']), message('JOB_ID', id_msg))

    proc.wait()

    ps = {
            'ret' : proc.returncode
            }

    log("Job ID", jid, "finished, notifying client")
    job_lock.acquire()

    job = jobs[jid]
    for i in range(len(job['addr'])):
        msg = message('JOB_EXIT', ps)
        send("{}:{}".format(job['addr'][i], job['port'][i]), msg)

    jobs.pop(jid, None)
    job_lock.release()

    log("Job ID", jid, "finished with return code", proc.returncode, level=1)

def job_request_handler(data):
    host = None
    for h in stats:
        if host == None or stats[h]['cpu'] < stats[host]['cpu']:
            host = h
    log("Distributing job to", host, level=1)
    send(host, message('JOB', data))

def job_term(data):
    job_lock.acquire()

    j = jobs.get(data['id'], None)
    if j:
        log("Terminating job:", j['id'], level=1)
        j['proc'].terminate()

    job_lock.release()

def job_resume_handler(data):
    job_lock.acquire()

    j = jobs.get(data['id'], None)
    if j is None:
        send("{}:{}".format(data['addr'], data['port']), message('JOB_DNE', None))
    else:
        log("Resuming job", data['id'], "from {}:{}".format(data['addr'],
            data['port']), level=1)
        j['addr'].append(data['addr'])
        j['port'].append(data['port'])
        send("{}:{}".format(data['addr'], data['port']), message('JOB_RES_ACC', None))

    job_lock.release()

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
        self.csock.close()

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
        elif mt == MSGT['JOB_TERM']:
            job_term(data)
        elif mt == MSGT['JOB_RES']:
            job_resume_handler(data)

def collect_stats():
    def cpu_load():
        s = subprocess.check_output(["uptime"]).decode().strip()
        s = [n.strip() for n in s.split()][-3:][1][:-1] # Use 5-minute average load
        return float(s) / cpu_count()

    def free_mem():
        s = subprocess.check_output(["free"]).decode().split("\n")[1]
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
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as ssock:
        ssock.bind((socket.gethostname(), settings.PORT))
        ssock.listen()

        log("Server listening on:", ssock.getsockname(), level=1)
        while True:
            csock, addr = ssock.accept()
            log("Incoming connection from", addr)
            st = server(csock, addr)
            st.start()

class client:
    sock = None
    retcode = 0
    def __init__(self, servers = None, user = None, cmd = None, log = None,
            cwd = None, node = None, id = None):

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((socket.gethostname(), 0))
        self.sock.listen()
        addr = self.sock.getsockname()

        self.addr = addr[0]
        self.port = addr[1]

        if node:
            self.node = node
            self.id = id
        else:
            if type(servers) is str:
                servers = [servers]
            self.servers = servers

            self.req = {
                    'user' : user,
                    'cmd' : cmd,
                    'log' : log,
                    'cwd' : cwd,
                    'addr' : addr[0],
                    'port' : addr[1]
                    }

            self.id_lock = threading.Lock()
            self.id_lock.acquire()

    def close(self):
        if not self.sock is None:
            self.sock.close()
            self.sock = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def hmsg(self, sock):
        msg = recv(sock)

        if msg is None:
            return

        mt = msg[0]
        data = msg[1]

        if mt == MSGT['JOB_ID']:
            self.node = data['node']
            self.id = data['id']
            self.id_lock.release()
        elif mt == MSGT['JOB_EXIT']:
            self.retcode = data['ret']
            return True
        elif mt == MSGT['JOB_RES_ACC']:
            return
        elif mt == MSGT['JOB_DNE']:
            return True

    def cll(self):
        while True:
            with self.sock.accept()[0] as csock:
                if self.hmsg(csock):
                    break
        self.close()

    def resume(self):
        self.thread = threading.Thread(target=self.cll)
        self.thread.start()

        data = {
                'id' : self.id,
                'addr' : self.addr,
                'port' : self.port
                }

        send(self.node, message('JOB_RES', data), block=True)

    def start(self):
        self.thread = threading.Thread(target=self.cll)
        self.thread.start()

        for server in self.servers:
            if send(server, message('JOB_REQ', self.req), block=True):
                break

    def get_id(self):
        self.id_lock.acquire()
        self.id_lock.release()
        return (self.node, self.id)

    def join(self):
        self.thread.join()

    def run(self):
        self.start()
        self.join()

    def terminate(self):
        send(self.node, message('JOB_TERM', { 'id' : self.id }))

if __name__ == '__main__':
    hearbeat().start()
    cleaner().start()
    listen_loop()
