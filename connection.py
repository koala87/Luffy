#coding=utf-8

"""Connection"""

__author__ = "Yingqi Jin <jinyingqi@luoha.com>"

import json
import time
import socket
import logging
from struct import pack, unpack


HEADER_LENGTH = 24

BUSINESS_HEADER_LENGTH = 53 

REGISTER_INFO_LENGTH = 36


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
        self.read_header()


    def on_close(self):
        self._stream.close()
        Connection.clients.remove(self)


class BoxConnection(Connection):
    box_clients = set()
    def __init__(self, stream, address):
        Connection.__init__(self, stream, address)
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
        InitConnection.init_clients.add(self)
        logging.debug('new init connection # %d from %s' % (len(InitConnection.init_clients), get_addr_str(address)))

    def on_close(self):
        Connection.on_close(self)
        InitConnection.init_clients.remove(self)
        logging.debug('init connection %s disconnected' % get_addr_str(self._address))


class BusinessConnection(object):
    clients = {} # item : business : sockets 
    header_length = BUSINESS_HEADER_LENGTH

    @classmethod
    def clean_connection(cls):
        for cli in cls.clients:
            cli._stream.close()


    def __init__(self, stream, address):
        Connection.clients.add(self)
        self._stream = stream
        self._address = address
        self._addr_str = get_addr_str(self._address) 
        self._registed = False

        self._header = ''
        self._body = ''

        self._type = '' # 1 bytes, app/box/erp/init
        self._id= '' # 4 bytes, unique id
        self._md5 = '' # 32 bytes, used to track each request
        self._timestamp = 0 # 8 bytes
        self._length = 0 # 4 bytes, body length
        self._unused = 0 # 4 bytes

        self._stream.set_close_callback(self.on_close)
        self.read_register_header()


    def read_register_header(self):
        self._stream.read_bytes(REGISTER_INFO_LENGTH,
            self.read_register_body) 


    def read_register_body(self, header):
        parts = unpack("I32s", header)
        self._length, self._md5 = parts
        self._length = socket.ntohl(self._length)
        logging.debug('read register header: %d %s from %s' % (self._length, self._md5, self._addr_str))
        self._stream.read_bytes(self._length, self.send_register_feedback)

    
    def send_register_feedback(self, msg):
        logging.debug('read register body: %d: %s from %s' % (len(msg), msg, self._addr_str))
        reply = {}
        reply['status'] = 0
        reply['reason'] = ''

        body = json.loads(msg)
        if 'function' not in body or 'timestamp' not in body:
            err = 'unsupported register info: %s' % msg
            reply['status'] = 1
            reply['reason'] = err
            logging.error('unsupported register info')
        else:
            function = body['function']
            timestamp = body['timestamp']
            time_cost = time.time() - timestamp 
            logging.info('register %s successfully for %s %.4f s' % (function, self._addr_str, time_cost))

            if function in BusinessConnection.clients:
                BusinessConnection.clients[function].add(self)
            else:
                new_business = set()
                new_business.add(self)
                BusinessConnection.clients[function] = new_business


        reply_str = json.dumps(reply)
        header = pack("I", socket.htonl(len(reply_str)))
        self._stream.write(header + reply_str)


    def read_header(self):
        self._stream.read_bytes(Connection.header_length, self.read_body)


    def read_body(self, header):
        self._header = header
        parts = unpack("cI32sdII", self._header)
        from socket import ntohl
        parts = [ntohl(x) for x in parts]

        (self._type, self._id, self._md5,
            self._timestamp, self._length, self._unused) = parts
        logging.debug('read header(%d, %d, %s, %f, %d, %d) from %s' % (
            self._type, self._id, self._md5,
            self._timestamp, self._length, self._unused,
            self._addr_str))

        self._stream.read_bytes(self._length, self.parse_main)


    def parse_main(self, body):
        logging.debug('read body(%s) from %s' % (body, self._addr_str))
        self._body = body
        pass


    def on_close(self):
        self._stream.close()
        Connection.clients.remove(self)


