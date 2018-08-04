## All intervals are in seconds
# File containing a list of peers
PEERS = "peers"
# Port to listen on
PORT = 8125
# Interval at which to send heartbeat to peers
HEARTBEAT = 1
# Pre-determined message size header size in bytes
MESSAGE_SIZE = 8
# Socket recieve buffer size
RECEIVE_BUFF = 1024
# How often to clean stats from stale peers
CLEAN_INTERVAL = 5
# Max time that a peer will still remain active before being removed for not
# sending a hearbeat
CLEAN_TIMEOUT = 10
