#!/usr/bin/env python2.7

from tornado import process
from tornado.tcpserver import TCPServer
from tornado.netutil import bind_sockets
from tornado.ioloop import IOLoop


if __name__ == '__main__':

    sockets = bind_sockets(8888)
    process.fork_processes(0)
    server = TCPServer()
    server.add_sockets(sockets)
    IOLoop.current().start()

