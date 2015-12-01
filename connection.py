#coding=utf-8

"""Connection"""

__author__ = "Yingqi Jin <jinyingqi@luoha.com>"

import json
import time
import socket
import logging
import threading
from struct import pack, unpack

from config import get_server
from bconnection import BusinessConnection

HEADER_LENGTH = 24

def get_addr_str(addr):
    return '%s:%d' % (addr[0], addr[1])


class Connection(object):
    clients = set()
    header_length = HEADER_LENGTH 

    @classmethod
    def clean_connection(cls):
        for cli in cls.clients:
            cli._stream.close()

    def __init__(self, stream, address):
        Connection.clients.add(self)
        self._stream = stream
        self._address = address
        self._addr_str = get_addr_str(self._address) 
        self._type = 1 # app:1 box:2 erp:3 init:4

        self._header = ''
        self._body = ''
        self._author = ''
        self._version = ''
        self._request = 0
        self._length = 0
        self._verify = 0
        self._device = 0

        self._stream.set_close_callback(self.on_close)
        self.read_header()


    def set_type(self, device_type):
        self._type = device_type


    def read_header(self):
        self._stream.read_bytes(Connection.header_length, self.read_body)


    def read_body(self, header):
        self._header = header
        parts = unpack("6I", self._header)
        from socket import ntohl
        parts = [ntohl(x) for x in parts]

        (self._author, self._version, self._request,
            self._verify, self._length, self._device) = parts
        logging.debug('read header(%d, %d, %d, %d, %d, %d) from %s' % (
            self._author, self._version, self._request,
            self._verify, self._length, self._device,
            self._addr_str))

        self._stream.read_bytes(self._length, self.parse_main)


    def parse_main(self, body):
        logging.debug('read body(%s) from %s' % (body, self._addr_str))
        self._body = body

        business = get_server(self._request)        

        BusinessConnection.clients_lock.acquire()
        if business in BusinessConnection.clients:
            logging.debug('forward request to %s' % business)
            conn = BusinessConnection.clients[business].pop()
            BusinessConnection.clients[business].add(conn)
            conn.send(self._header + self._body, self._type, self._address[0])
        else:
            logging.debug('no %s business server is avaliable' % business)
        BusinessConnection.clients_lock.release()

        self.read_header()


    def on_close(self):
        self._stream.close()
        Connection.clients.remove(self)


class BoxConnection(Connection):
    box_clients = set()
    def __init__(self, stream, address):
        Connection.__init__(self, stream, address)
        self.set_type(2)
        BoxConnection.box_clients.add(self)
        logging.debug('new box connection # %d from %s' % (len(BoxConnection.box_clients), get_addr_str(address)))
    
    def on_close(self):
        Connection.on_close(self)
        BoxConnection.box_clients.remove(self)
        logging.debug('box connection %s disconnected' % get_addr_str(self._address))


class AppConnection(Connection):
    app_clients = set()
    def __init__(self, stream, address):
        Connection.__init__(self, stream, address)
        self.set_type(1)
        AppConnection.app_clients.add(self)
        logging.debug('new app connection # %d from %s' % (len(AppConnection.app_clients), get_addr_str(address)))

    def on_close(self):
        Connection.on_close(self)
        AppConnection.app_clients.remove(self)
        logging.debug('app connection %s disconnected' % get_addr_str(self._address))


class ERPConnection(Connection):
    erp_clients = set()
    def __init__(self, stream, address):
        Connection.__init__(self, stream, address)
        self.set_type(3)
        ERPConnection.erp_clients.add(self)
        logging.debug('new erp connection # %d from %s' % (len(ERPConnection.erp_clients), get_addr_str(address)))

    def on_close(self):
        Connection.on_close(self)
        ERPConnection.erp_clients.remove(self)
        logging.debug('erp connection %s disconnected' % get_addr_str(self._address))


class InitConnection(Connection):
    init_clients = set()
    def __init__(self, stream, address):
        Connection.__init__(self, stream, address)
        self.set_type(4)
        InitConnection.init_clients.add(self)
        logging.debug('new init connection # %d from %s' % (len(InitConnection.init_clients), get_addr_str(address)))

    def on_close(self):
        Connection.on_close(self)
        InitConnection.init_clients.remove(self)
        logging.debug('init connection %s disconnected' % get_addr_str(self._address))
