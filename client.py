#!/usr/bin/python
"""Client code modified from phase 2."""

import argparse
import common
import common2
import time


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

    args = parser.parse_args()

    if args.cmd in ['query_servers', 'lock_get', 'lock_release', 'setr',
                    'getr']:
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
    else:
        response = common.send_receive_range(args.server, common2.SERVER_LOW,
                                             common2.SERVER_HIGH, vars(args))
        print response

if __name__ == "__main__":
    main()
