#!/usr/bin/env python2.7
#!coding=utf-8

"""control module"""

__author__ = 'Yingqi Jin <jinyingqi@luoha.com>'

import os
import sys
import signal
import logging
from tornado.ioloop import IOLoop

# add generic dir into sys path
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(ROOT, '../generic'))
from utility import init_log, get_default_log, get_ip 
from business import Business


def sig_handler(sig, frame):
    IOLoop.current().stop()
    logging.info('stop ...')


class Control(Business):
    control_clients = set()

    def __init__(self, ip, port):
        Control.control_clients.add(self)
        Business.__init__(self, 'control', ip, port)


    # overwrite process method
    def process(self):
        logging.debug('in control process ...')


    def on_close(self):
        Control.control_clients.remove(self)
        Business.on_close(self)


def register_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-i", "--host", dest="host",
        default=get_ip(), help="specify host, default is local ip")
    parser.add_option("-p", "--port", dest="port",
        type="int",
        default=6666, help="specify port, default is 6666")
    parser.add_option("-n", "--num", dest="num",
        type="int",
        default=1, help="specify threads num, default is 10")
    parser.add_option("-l", "--log", dest="log",
        default=get_default_log(), help="specify log name")
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

    logging.info('start %d connections to server %s:%d ...' % (opts.num, opts.host, opts.port))

    for i in xrange(opts.num):
        Control(opts.host, opts.port)
    
    IOLoop.current().start()

    logging.info('stop ...')

