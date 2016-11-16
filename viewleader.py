#!/usr/bin/python

"""Modified from phase 2."""

import time
import hashlib
import socket
import collections
import common
import common2

##############
# Globals

# Stores global configuration variables
config = {
    "epoch": 0,
    # List of expired leases
    "expired": []
}

# Stores all server leases sorted by hash, entries have form:
# {"lockid": lockid,
#  "requestor": requestor,
#  "timestamp": time.time(),
#  'hash': common.hash_number(requestor)}
leases = []

# Store all locks
locks = []

###################


def arc(index, low, high, list_):
    """Return an arc of the leases circle.

    [index-low, index+high]
    """
    len_ = len(list_)
    index1 = (index - low) % len_
    index2 = (index + high) % len_
    if index1 < index2:
        return list_[index1:index2 + 1]
    else:
        return list_[:index1 + 1] + list_[index2:index1]


def rebalance_request(keys, origin, dests):
    """Request to send keys from servers to a server."""
    success = False
    for svr in origin:
        host, port = svr.split(':')
        res = common.send_receive(host, port, {'cmd': rebalance_request,
                                               'keys': keys, 'dest': dests})
        if res['reply'] == 'ok':
            success = True
            break
        # # Try to send the key to both servers from the last server.
        # elif res['reply'] == 'KeyError':
        #     dests.append(svr)
    return {'reply': 'ok'} if success else {'reply': 'fail'}


def rebalancing_lost(lost_lease_index_hash):
    """Rebalance the keys and values when a server is lost."""
    print 'Rebalancing...'
    lost_lease_index, lost_lease_hash = lost_lease_index_hash
    count = collections.defaultdict(list)
    # index1 = (lost_lease_index - 3) % len_
    # index2 = (lost_lease_index + 2) % len_
    # if index1 < index2:
    #     adjacent_leases = leases[index1:index2+1]
    # else:
    #     adjacent_leases = leases[index1:] + leases[:index2+1]
    adjacent_leases = arc(lost_lease_index, 3, 2, leases)
    print adjacent_leases
    for lease in adjacent_leases:
        host, port = lease['lockid'].split(':')
        res = common.send_receive(host, port, {'cmd': 'query_all_keys'})
        print 'res'
        print res
        for key in res['result']:
            count[key].append(lease['hash'])
    print count
    # We only need to update a server if there are 2 of a key in
    # adjacent_leases
    to_add = {key: value for (key, value) in count.iteritems()
              if len(value) == 2}
    print to_add
    # These are the keys we need to add to the next server, the one after
    # that, and the last one, respectively. Could probably be own function.
    to_add_1 = []
    to_add_2 = []
    to_add_3 = []
    for (key, hashes) in to_add.iteritems():
        if hashes[0] < lost_lease_hash:
            if hashes[1] < lost_lease_hash:
                to_add_1.append(key)
            else:
                to_add_2.append(key)
        else:
            to_add_3.append(key)
    print 'ready to rebalance'
    svrs_for_1 = arc(lost_lease_index, 2, -1, leases)
    print svrs_for_1
    assert len(svrs_for_1) == 2
    rebalance_request(to_add_1, svrs_for_1, adjacent_leases[4])
    svrs_for_2 = arc(lost_lease_index, 1, 1, leases)
    print svrs_for_2
    rebalance_request(to_add_2, svrs_for_2, adjacent_leases[5])
    svrs_for_3 = arc(lost_lease_index, -1, 2, leases)
    print svrs_for_3
    rebalance_request(to_add_3, svrs_for_3, adjacent_leases[6])
    print 'Rebalancing complete'



def rebalancing_new():
    """Rebalance the keys and values when there is a new server."""
    remove_expired_leases()


def add_lease(leases, lockid, requestor):
    """Add new lease to leases."""
    leases.append({"lockid": lockid, "requestor": requestor,
                   "timestamp": time.time(),
                   'hash': common.hash_number(str(requestor))})
    # keep leases sorted to keep setr cost down
    # I'm not sure what algorithm python uses, but I think insertion sort
    # would be best here.
    return sorted(leases, key=lambda d: d['hash'])
###################
# RPC implementations


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
                    # add hash and resort leases
                    lease['hash'] = common.hash_number(str(requestor))
                    # This seems suspicious because I am changing leases as
                    # I'm iterating through it, but I exit right away, so
                    # it should be fine.
                    global leases
                    leases = sorted(leases, key=lambda d: d['hash'])
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
        leases = add_lease(leases, lockid, requestor)
        config["epoch"] += 1
        return {"status": "ok", "epoch": config["epoch"]}


def remove_expired_leases():
    """Check which leases have already expired."""
    global leases
    expired = None
    new_leases = []
    i = 0
    for lease in leases:
        if time.time() - lease['timestamp'] <= common2.LOCK_LEASE:
            new_leases.append(lease)
        else:
            config["expired"].append(lease["requestor"])
            expired = (i, lease['hash'])
        # keep track of the index
        i += 1
    leases = new_leases
    if expired:
        config["epoch"] += 1
        # The rebalancing only works if one server dies at a time.
        rebalancing_lost(expired)


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
