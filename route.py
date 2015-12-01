#!/usr/bin/env python2.7
#coding=utf-8

"""Lohas route server"""

__author__ = "Yingqi Jin <jinyingqi@luoha.com>"

import sys
import time
import signal
import logging
from tornado.tcpserver import TCPServer
from tornado.ioloop import IOLoop

from connection import Connection, AppConnection, BoxConnection
from connection import ERPConnection, InitConnection
from bconnection import BusinessConnection

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


def init_log(fname, debug=False):
    """file log level is debug, stdout log level depends on argument"""
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s - %(process)-6d - %(threadName)-10s - %(levelname)-8s] %(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S',
        filename=fname,
        filemode='w')
    
    sh = logging.StreamHandler()
    if debug:
        sh.setLevel(logging.DEBUG)
    else:
        sh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)-8s %(message)s')
    sh.setFormatter(formatter)
    logging.getLogger('').addHandler(sh)


def get_default_log():
    """return default log name, eg, simple_server.py -> simple_server.log"""
    import os
    name = os.path.basename(sys.argv[0])
    pos = name.rfind('.')
    if pos != -1:
        name = name[:pos]
    return name + '.log'


def register_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-i", "--host", dest="host",
        default="localhost", help="specify host, default is localhost")
    parser.add_option("-p", "--port", dest="port",
        type="int",
        default=3050, help="specify port, default is 3050")
    parser.add_option("-n", "--num", dest="num",
        type="int",
        default=1, help="specify process num")
    parser.add_option("-l", "--log", dest="log",
        default=get_default_log(),  help="specify process num")
    parser.add_option("-d", "--debug", dest="debug",
        action='store_true',
        default=False, help="dump debug info to stdout")

    (options, args) = parser.parse_args() 
    return options


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
