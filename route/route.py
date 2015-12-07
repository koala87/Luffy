#!/usr/bin/env python2.7
#coding=utf-8

"""Lohas route server"""

__author__ = "Yingqi Jin <jinyingqi@luoha.com>"

import os
import sys
import time
import signal
import logging
from tornado.tcpserver import TCPServer
from tornado.ioloop import IOLoop

from connection import Connection, AppConnection, BoxConnection
from connection import ERPConnection, InitConnection
from bconnection import BusinessConnection

# add generic dir into sys path
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(ROOT, '../generic'))
from utility import init_log, get_default_log, get_ip 

# listen port
BOX_PORT = 58849
ERP_PORT = 25377
APP_PORT = 3050
INIT_PORT = 11235
BUSINESS_PORT = 6666

LISTEN_PORT = {
    BOX_PORT : 'box',
    ERP_PORT : 'erp',
    APP_PORT : 'app',
    INIT_PORT : 'init',
    BUSINESS_PORT : 'business',
}

# handle_stream will be called once new connection is created
class KTVServer(TCPServer):
    def handle_stream(self, stream, address):
        ip, port = stream.socket.getsockname()
        port_conn_map = {
            BOX_PORT : BoxConnection,
            APP_PORT : AppConnection,
            ERP_PORT : ERPConnection,
            INIT_PORT : InitConnection,
            BUSINESS_PORT : BusinessConnection,
        }
        # instance new connection based on port type
        port_conn_map[port](stream, address)


def sig_handler(sig, frame):
    IOLoop.current().stop()
    Connection.clean_connection()
    logging.info('stop server ...')


def register_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-i", "--host", dest="host",
        default=get_ip(), help="specify host, default is local ip")
    parser.add_option("-l", "--log", dest="log",
        default=get_default_log(), help="specify log name")
    parser.add_option("-n", "--num", dest="num",
        type=int,
        default=1, help="specify process num")
    parser.add_option("-d", "--debug", dest="debug",
        action='store_true',
        default=False, help="enable debug")

    (options, args) = parser.parse_args() 
    return options


if __name__ == '__main__':
    
    opts = register_options()

    init_log(opts.log, opts.debug)

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    logging.info('start server ...')
    logging.info('log file %s ...' % opts.log)

    server = KTVServer()

    for port, pstr in LISTEN_PORT.iteritems():
        server.bind(port, opts.host)
        logging.info('listen %s port %d for %s ...' % (opts.host, port, pstr))

    server.start(opts.num)

    IOLoop.current().start()
