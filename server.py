#!/usr/bin/python
"""Server code modified from phase 2."""

import common
import common2
import random
import argparse
import time

##############
# Globals

# Stores global configuration variables
config = {"epoch": None,
          "port": None,
          "server_hash": random.random(),
          "last_heartbeat": None}

# Stores shared values for get and set commands
store = {}

# Keys waiting commit
keys = []


def update_lease():
    """Request or extend a lease from viewleader."""
    if config["port"] is None:
        return {}
    config["last_heartbeat"] = time.time()
    res = common.send_receive_range(config["viewleader"],
                                    common2.VIEWLEADER_LOW,
                                    common2.VIEWLEADER_HIGH,
                                    {"cmd": "heartbeat",
                                     "port": config["port"],
                                     "requestor": config["server_hash"]})
    if "error" in res:
        print "Can't update lease: %s" % res["error"]
        return res
    if res.get("status") == 'ok':
        if config["epoch"] is not None and res["epoch"] < config["epoch"]:
            print "Received invalid epoch (%s < %s)" % (res["epoch"],
                                                        config["epoch"])
            return {"error": "bad epoch"}
        else:
            config["epoch"] = res["epoch"]
    else:
        print "Can't renew lease: %s" % res["status"]
        return res
    return {}

###################
# RPC implementations


def init(msg, addr):
    """Init function - nop."""
    config["port"] = msg["port"]
    update_lease()
    return {}


def set_val(msg, addr):
    """set command sets a key in the value store."""
    key = msg["key"]
    val = msg["val"]
    store[key] = {"val": val}
    print "Setting key %s to %s in local store" % (key, val)
    return {"status": "ok"}


def get_val(msg, addr):
    """fetch a key in the value store."""
    key = msg["key"]
    if key in store:
        print "Querying stored value of %s" % key
        return {"status": "ok", "value": store[key]["val"]}
    else:
        print "Stored value of key %s not found" % key
        return {"status": "not_found"}


def query_all_keys(msg, addr):
    """Return all keys in the value store."""
    print "Returning all keys"
    keyvers = [key for key in store.keys()]
    return {"result": keyvers}


def print_something(msg, addr):
    """Print a message in response to print command."""
    print "Printing %s" % " ".join(msg["text"])
    return {"status": "ok"}


def tick(msg, addr):
    """accept timed out - nop."""
    return {}


def setr_request(msg, addr):
    """Vote for setr."""
    key = msg['key']
    if key in keys:
        return {'reply': 'no'}
    else:
        keys.append(key)
        return {'reply': 'ok'}


def setr(msg, addr):
    """Successfully set."""
    set_val(msg, addr)
    keys.remove(msg['key'])
    return {}


def setr_deny(msg, addr):
    """Cancel setr."""
    keys.remove(msg['key'])
    return {}
##############
# Main program


def handler(msg, addr):
    """RPC dispatcher invokes appropriate function."""
    cmds = {
        "init": init,
        "set": set_val,
        "get": get_val,
        "print": print_something,
        "query_all_keys": query_all_keys,
        "timeout": tick,
        'setr_request': setr_request,
        'setr': setr,
        'setr_deny': setr_deny
    }
    res = cmds[msg["cmd"]](msg, addr)

    # Conditionally send heartbeat
    if time.time() - config["last_heartbeat"] >= 10:
        update_lease()

    return res


def main():
    """Server entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--viewleader', default='localhost')
    args = parser.parse_args()
    config["viewleader"] = args.viewleader

    for port in range(common2.SERVER_LOW, common2.SERVER_HIGH):
        print "Trying to listen on %s..." % port
        result = common.listen(port, handler, 10)
        print result
    print "Can't listen on any port, giving up"

if __name__ == "__main__":
    main()
