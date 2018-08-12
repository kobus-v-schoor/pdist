import os

DIR = os.path.dirname(os.path.abspath(__file__))
HOME = "/var/www"

## All intervals are in seconds
# File containing a list of peers
PEERS = os.path.join(DIR, "peers")
# Port to listen on
PORT = 8125
# Interval at which to send heartbeat to peers
HEARTBEAT = 30
# Pre-determined message size header size in bytes
MESSAGE_SIZE = 8
# Socket recieve buffer size
RECEIVE_BUFF = 1024
# How often to clean stats from stale peers
CLEAN_INTERVAL = 45
# Max time that a peer will still remain active before being removed for not
# sending a hearbeat
CLEAN_TIMEOUT = 90
# Logging level 0 = debug, 1 = info, 2 = warn
LOG = 1
# PSK file
PSK = os.path.join(os.environ.get('HOME', HOME), ".pdist_psk")
