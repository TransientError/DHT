#!/usr/bin/python

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

# Stores shared values for get and se commands
store = {}

# Request or extend a lease from view leaer
def update_lease():
    if config["port"] is None:
        return {}
    config["last_heartbeat"] = time.time()
    res = common.send_receive_range(config["viewleader"], common2.VIEWLEADER_LOW , 
        common2.VIEWLEADER_HIGH , {
            "cmd": "heartbeat",
            "port": config["port"],
            "requestor": config["server_hash"],
        })
    if "error" in res:
        print "Can't update lease: %s" % res["error"]
        return res
    if res.get("status") == 'ok':
        if config["epoch"] is not None and res["epoch"] < config["epoch"]:
            print "Received invalid epoch (%s < %s)" % (res["epoch"], config["epoch"])
            return {"error": "bad epoch"}
        else:
            config["epoch"] = res["epoch"]
    else:
        print "Can't renew lease: %s" % res["status"]
        return res
    return {}

###################
# RPC implementations

# Init function - nop
def init(msg, addr):
    config["port"] = msg["port"]
    update_lease()
    return {}

# set command sets a key in the value store
def set_val(msg, addr):
    key = msg["key"]
    val = msg["val"]
    store[key] = {"val": val}
    print "Setting key %s to %s in local store" % (key, val)
    return {"status": "ok"}

# fetches a key in the value store
def get_val(msg, addr):
    key = msg["key"]
    if key in store:
        print "Querying stored value of %s" % key
        return {"status": "ok", "value": store[key]["val"],}
    else:
        print "Stored value of key %s not found" % key
        return {"status": "not_found"}

# Returns all keys in the value store
def query_all_keys(msg, addr):
    print "Returning all keys"
    keyvers = [ key for key in store.keys() ]
    return {"result": keyvers}

# Print a message in response to print command
def print_something(msg, addr):
    print "Printing %s" % " ".join(msg["text"])
    return {"status": "ok"}

# accept timed out - nop
def tick(msg, addr):
    return {}

##############
# Main program

# RPC dispatcher invokes appropriate function
def handler(msg, addr):
    cmds = {
        "init": init,
        "set": set_val,
        "get": get_val,
        "print": print_something,
        "query_all_keys": query_all_keys,
        "timeout": tick,
    }
    res =  cmds[msg["cmd"]](msg, addr)

    # Conditionally send heartbeat
    if time.time() - config["last_heartbeat"] >= 10:
        update_lease()

    return res

# Server entry point
def main():
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