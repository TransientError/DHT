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


def set_val(lockid, key, val):
    """set value on a server."""
    svr_addr, svr_port = lockid.split(':')
    msg = {'cmd': 'setr', 'key': key, 'val': val}
    print 'set %s to %s' % (key, val)
    return common.send_receive(svr_addr, svr_port, msg)


def find_svrs(key, hashes):
    """find the servers to set in."""
    key_hash = common.hash_number(key)
    len_ = len(hashes)
    print len_
    if len_ > 3:
        for i in xrange(len_):
            if key_hash < hashes[i][0]:
                pass
            else:
                return [hashes[i], hashes[(i + 1) % len_],
                        hashes[(i + 2) % len_]]
        return hashes[:2]
    else:
        return hashes


def setr_request(lockid, key):
    """Request votes from servers."""
    svr_addr, svr_port = lockid.split(':')
    msg = {'cmd': 'setr_request', 'key': key}
    return common.send_receive(svr_addr, svr_port, msg)


def setr_deny(svrs, key):
    """setr not successful."""
    deny = {'cmd': 'setr_deny', 'key': key}
    for svr in svrs:
        svr_addr, svr_port = svr.split(':')
        common.send_receive(svr_addr, svr_port, deny)
    return {'status': 'setr not successful'}


def setr_accept(svrs, key, val):
    """setr successful."""
    for svr in svrs:
        set_val(svr, key, val)
    return {'status': 'setr successful'}
###################
# RPC implementations


def setr(msg, addr):
    """replicated set."""
    key, val = msg['key'], msg['val']
    hashes = sorted([(entry['hash'], entry['lockid']) for entry in leases])
    svrs = [svr[1] for svr in find_svrs(key, hashes)]
    print svrs
    ress = [setr_request(svr, key) for svr in svrs]
    # we iterate through ress twice, which is a bit inefficient, but ress
    # should only be around 3 items, so it should be ok.
    if 'no' in [res['reply'] for res in ress]:
        res = setr_deny(svrs, key)
    elif any([res['epoch'] != config['epoch'] for res in ress]):
        res = setr_deny(svrs, key)
    else:
        res = setr_accept(svrs, key, val)
    return res


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
                    lease['hash'] = common.hash_number(str(requestor))
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
        leases.append({"lockid": lockid, "requestor": requestor,
                       "timestamp": time.time(),
                       'hash': common.hash_number(str(requestor))})
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
        ip = lease["lockid"], lease['hash']
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
