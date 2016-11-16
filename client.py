#!/usr/bin/python
"""Client code modified from phase 2."""

import argparse
import socket
import common
import common2
import time


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
    if len_ > 3:
        for i in xrange(len_):
            if key_hash < hashes[i][0]:
                pass
            else:
                return [hashes[i], hashes[(i + 1) % len_],
                        hashes[(i + 2) % len_]]
        return hashes[:3]
    else:
        return hashes


def setr_request(lockid, key):
    """Request votes from servers."""
    svr_addr, svr_port = map(str, lockid.split(':'))
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


def main():
    """Client entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', default='localhost')
    parser.add_argument('--viewleader', default='localhost')

    subparsers = parser.add_subparsers(dest='cmd')

    parser_set = subparsers.add_parser('set')
    parser_set.add_argument('key', type=str)
    parser_set.add_argument('val', type=str)

    parser_get = subparsers.add_parser('get')
    parser_get.add_argument('key', type=str)

    parser_print = subparsers.add_parser('print')
    parser_print.add_argument('text', nargs="*")

    parser_query = subparsers.add_parser('query_all_keys')
    parser_server_query = subparsers.add_parser('query_servers')

    parser_lock_get = subparsers.add_parser('lock_get')
    parser_lock_get.add_argument('lockid', type=str)
    parser_lock_get.add_argument('requestor', type=str)

    parser_lock_get = subparsers.add_parser('lock_release')
    parser_lock_get.add_argument('lockid', type=str)
    parser_lock_get.add_argument('requestor', type=str)

    parser_setr = subparsers.add_parser('setr')
    parser_setr.add_argument('key', type=str)
    parser_setr.add_argument('val', type=str)

    parser_getr = subparsers.add_parser('getr')
    parser_getr.add_argument('key', type=str)

    args = parser.parse_args()

    if args.cmd in ['query_servers', 'lock_get', 'lock_release']:
        while True:
            response = common.send_receive_range(args.viewleader,
                                                 common2.VIEWLEADER_LOW,
                                                 common2.VIEWLEADER_HIGH,
                                                 vars(args))
            if response.get("status") == "retry":
                print "Waiting on lock %s..." % args.lockid
                time.sleep(5)
                continue
            else:
                break
        print response

    elif args.cmd == 'setr':
        key, val = args.key, args.val
        print 'Trying to setr %s to %s' % (key, val)
        msg = common.send_receive_range(args.viewleader,
                                        common2.VIEWLEADER_LOW,
                                        common2.VIEWLEADER_HIGH,
                                        {'cmd': 'query_servers'})
        # results are lockid, hash
        hashes = [(entry[1], entry[0])
                  for entry in msg['result']]
        # list just the lockids
        svrs = [svr[1] for svr in find_svrs(key, hashes)]
        ress = [setr_request(svr, key) for svr in svrs]
        # we iterate through ress twice, which is a bit inefficient, but ress
        # should only be around 3 items, so it should be ok.
        if 'no' in [res['reply'] for res in ress]:
            res = setr_deny(svrs, key)
            print 'key already being set'
        elif any([res['epoch'] != msg['epoch'] for res in ress]):
            res = setr_deny(svrs, key)
            print 'server with wrong epoch'
        else:
            res = setr_accept(svrs, key, val)
        return res
    elif args.cmd == 'getr':
        key = args.key
        msg = common.send_receive_range(args.viewleader,
                                        common2.VIEWLEADER_LOW,
                                        common2.VIEWLEADER_HIGH,
                                        {'cmd': 'query_servers'})
        # results are lockid, hash
        hashes = [(entry[1], entry[0]) for entry in msg['result']]
        # list the lockids
        svrs = [svr[1] for svr in find_svrs(key, hashes)]
        for svr in svrs:
            host, port = svr.split(':')
            new_msg = {'cmd': 'get', 'key': key}
            try:
                print common.send_receive(host, port, new_msg)
                break
            except socket.Timeouterror:
                pass
                print {'status': 'unable to retrieve %s from any server' % key}
    else:
        response = common.send_receive_range(args.server, common2.SERVER_LOW,
                                             common2.SERVER_HIGH, vars(args))
        print response

if __name__ == "__main__":
    main()
