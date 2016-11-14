#!/usr/bin/python

"""Modified from phase 2."""

import time
import hashlib
import common
import common2

##############
# Globals

# Stores global configuration variables
config = {
    "epoch": 0,
    "lowest_hash": (None),
    # List of expired leases
    "expired": []
}

# Stores all server leases, entries have form:
# {"lockid": lockid,
#  "requestor": requestor,
#  "timestamp": time.time(),
#  'hash': common.hash_number(requestor)}
leases = []

# Store all locks
locks = []

###################
def set_val(lockid, val):
    svr_addr, svr_port = lockid.split(':')
    print svr_addr
    print svr_port
###################
# RPC implementations


def setr(msg, addr):
    """replicated set."""
    key, val = msg['key'], msg['val']
    key_hash = common.hash_number(key)
    print leases
    hashes = sorted([(entry['hash'], entry['lockid']) for entry in leases])
    print hashes
    value_set = False
    for hash_ in hashes:
        if key_hash < hash_[0]:
            pass
        else:
            value_set = True
            set_val(hash_[1], val)
    if not value_set:
        set_val(config['lowest_hash'][1], val)
    return {}


def getr(msg, addr):
    """replicated get."""
    pass


def lock_get(msg, addr):
    """Try to acquire a lock."""
    lockid = msg["lockid"]
    requestor = msg["requestor"]
    for lock in locks:
        if lock["lockid"] == lockid:
            if len(lock["queue"]) == 0:
                lock["queue"].append(requestor)
                return {"status": "granted"}
            elif lock["queue"][0] == requestor:
                return {"status": "granted"}
            else:
                if requestor not in lock["queue"]:
                    lock["queue"].append(requestor)
                return {"status": "retry"}
    else:
        # this lock doesn't exist yet
        locks.append({"lockid": lockid, "queue": [requestor]})
        return {"status": "granted"}


def lock_release(msg, addr):
    """Release a held lock, or remove oneself from waiting queue."""
    lockid = msg["lockid"]
    requestor = msg["requestor"]
    for lock in locks:
        if lock["lockid"] == lockid:
            if requestor in lock["queue"]:
                lock["queue"].remove(requestor)
                return {"status": "ok"}
    else:
        return {"status": "unknown"}


def server_lease(msg, addr):
    """Manage requests for a server lease."""
    lockid = "%s:%s" % (addr, msg["port"])
    requestor = msg["requestor"]

    remove_expired_leases()

    if msg["requestor"] in config["expired"]:
        return {"status": "deny"}

    for lease in leases:
        if lease["lockid"] == lockid:
            # lease is present

            if time.time() - lease["timestamp"] > common2.LOCK_LEASE:
                # lease expired
                if lease["requestor"] == requestor:
                    # server lost lease, then recovered, but we deny it
                    return {"status": "deny"}
                else:
                    # another server at same address is okay
                    lease["timestamp"] = time.time()
                    lease["requestor"] = requestor
                    # add hash
                    hash_ = common.hash_number(str(requestor))
                    low = config['lowest_hash']
                    if not low or hash_ < low[0]:
                        config['lowest_hash'] = (hash_, lockid)
                    lease['hash'] = hash_
                    config["epoch"] += 1
                    return {"status": "ok", "epoch": config["epoch"]}
            else:
                # lease still active
                if lease["requestor"] == requestor:
                    # refreshing ownership
                    lease["timestamp"] = time.time()
                    return {"status": "ok", "epoch": config["epoch"]}
                else:
                    # locked by someone else
                    return {"status": "retry", "epoch": config["epoch"]}
    else:
        # lock not present yet
        hash_ = common.hash_number(str(requestor))
        low = config['lowest_hash']
        if not low or hash_ < low[0]:
            config['lowest_hash'] = (hash_, lockid)
        leases.append({"lockid": lockid, "requestor": requestor,
                       "timestamp": time.time(),
                       'hash': hash_})
        config["epoch"] += 1
        return {"status": "ok", "epoch": config["epoch"]}


def remove_expired_leases():
    """Check which leases have already expired."""
    global leases
    expired = False
    new_leases = []
    for lease in leases:
        if time.time() - lease['timestamp'] <= common2.LOCK_LEASE:
            new_leases.append(lease)
        else:
            config["expired"].append(lease["requestor"])
            expired = True
    if expired:
        config["epoch"] += 1
    leases = new_leases


def query_servers(msg, addr):
    """Output the set of currently active servers."""
    servers = []
    remove_expired_leases()
    for lease in leases:
        ip = lease["lockid"]
        servers.append(ip)

    return {"result": servers, "epoch": config["epoch"]}


def init(msg, addr):
    """init."""
    return {}

##############
# Main program


def handler(msg, addr):
    """RPC dispatcher invokes appropriate function."""
    cmds = {
        "init": init,
        "setr": setr,
        "getr": getr,
        "heartbeat": server_lease,
        "query_servers": query_servers,
        "lock_get": lock_get,
        "lock_release": lock_release,
    }

    return cmds[msg["cmd"]](msg, addr)


def main():
    """Viewleader entry point."""
    for port in range(common2.VIEWLEADER_LOW, common2.VIEWLEADER_HIGH):
        print "Trying to listen on %s..." % port
        result = common.listen(port, handler)
        print result
    print "Can't listen on any port, giving up"

if __name__ == "__main__":
    main()
