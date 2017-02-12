# Distributed Hash Table (Assignment 3)  
Kevin Wu
(modified from solutions to assignment 2 written by Jeff Epstein)  

These scripts rely on the following packages:  
* argparse
- threading
- socket
- struct
- json
- collections
- time
- random
- hashlib

## Instructions:
Python must be installed in the default location.  
These scripts were designed to be run on linux.  

viewleader.py is run with `./viewleader.py`  
The viewleader will try to bind to the first port available. The phase2
commands are more or less untouched. It's also in charge of the rebalancing.
Whenever there is a view change, the viewleader will start the rebalancing
processes on a separate thread, so it still answers heartbeats and query_servers in its main thread.

server.py can be run with `./server.py (--viewleader)`  
There are two functions
rebalancing_f for when a server fails, and rebalancing_a for when there is
an additional server. They are wrapped in rebalancing functions with the
full name that run the function on a separate thread. Only one server can
rebalance at the same time, so it needs to acquire a 'rebalancing' lock from
the viewleader.

client.py is run with: `./client.py (--server|--viewleader ip_addr) command args`

    The client recognizes four commands to the server:
        print (args)       -- prints the args (even if it has spaces, but no
                              escape characters)
        set (name) (value) -- Set a local variable (name) to (value) on the
                              server. 
        get (name)         -- Retrieve the value for (name) in the local
                              namespace on the server.
        query_all_keys     -- Retrieve all the names stored in the server's local
                              namespace.

    The client recognizes three commands to the viewleader:
        lock_get (lock) (client)     -- Try to get lock from the viewleader; if
                                        the lock is currently held by another
                                        client, retry every 5 seconds.
        lock_release (lock) (client) -- Release a held lock; doesn't work if you
                                        don't actually hold the lock.
        query_servers                -- Return a dict with the current epoch and
                                       list of servers.
        setr (name) (value)          -- This sets (name) to (value) on the
                                        server with the highest hash less than
                                        the key hash, and the two subsequent
                                        servers
        getr (name)                  -- This gets the value associated with
                                        (name) on the servers with the value
                                        from the server with the lowest hash
                                        value to have it.

## Possible Errors:
* The overall quality of the latter half, particularly the rebalancing when you
add a new server, is lower than I would like. I'm confident that it works on
3 servers or fewer. At 4 servers, it works sometimes, and for more than that
this rebalancing will probably fail from broken pipes or maybe some other
reason. The entire if section for more than 5 servers has never been tested.

* The rebalancing is only designed to work if you don't add or remove servers
during rebalancing. However, it seems to work even if you don't respect
that, but I can't make any guarantees.

* Setr and getr will fail if there is no viewleader or no servers.

* You can force getr to retrieve the wrong value by using the normal set.

## Known Deficiencies:
* The viewleader prints 'Rebalancing complete' before rebalancing is actually
    complete.

* There is a warning about using global leases and warning about iterating
    Nonetype, but the scripts seem to work just fine.

* If you put a server on the same ip address as viewleader, the viewleader
    will save its address as 127.0.0.1, so the client will look there for better
    or for worse.

## Design Choices and explanation of rebalancing algorithm:
* I used random to generate server ids, which I think is sufficiently random
    for our needs. I'm using SHA1 for the hashing algorithm and modding it by
    10000, because that was what is recommended in the assignment. It should
    return a number that is evenly distributed, so the three servers that are
    chosen should be approximately evenly distributed in the range of the
    function.

* When a server is lost, each server checks its neighboring servers for keys.
    Then it checks if it needs a copy of each key by their hashes. Every key
    that needs to be copied is then requested from the other servers. This way
    the viewleader will only have to supply each server with its neighboring
    servers, and let the servers do the rest of the work. If the number of
    servers is 3 or fewer each server copies all the keys that it finds in the
    two other servers.

* When there is a new server, when the number of servers is 3 or fewer, we do
    the same thing as when a server is lost. This rebalancing follows the same
    idea as the rebalancing when a server is lost, but it is more complicated.
    The viewleader gives each server a list of servers that are near it. Then
    each server decides using the key hashes if it needs to have a key that it
    doesn't already have. If the server does decide that it needs a new key, it
    asks the server holding the third copy of the value to send it over and then
    delete the key. In some cases when we reach the new server, we will need to
    get both the value of the key and send a third value of that key to the
    appropriate server. 
