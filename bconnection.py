#coding=utf-8

"""Connection"""

__author__ = "Yingqi Jin <jinyingqi@luoha.com>"

import json
import time
import socket
import logging
import threading
from struct import pack, unpack

BUSINESS_HEADER_LENGTH = 56 

REGISTER_INFO_LENGTH = 36


def get_addr_str(addr):
    return '%s:%d' % (addr[0], addr[1])


class BusinessConnection(object):
    clients = {} # item : business : sockets 
    conns = set()
    clients_lock = threading.Lock()
    header_length = BUSINESS_HEADER_LENGTH

    @classmethod
    def clean_connection(cls):
        for cli in cls.clients:
            cli._stream.close()


    def __init__(self, stream, address):
        BusinessConnection.conns.add(self)
        self._stream = stream
        self._address = address
        self._addr_str = get_addr_str(self._address) 
        self._registed = False

        self._header = ''
        self._body = ''

        self._function = '' # control/forward/music ...

        # header between route and business
        self._type = '' # 1 bytes, app/box/erp/init
        self._id= '' # 4 bytes, unique id
        self._md5 = '' # 32 bytes, used to track each request
        self._timestamp = 0 # 8 bytes
        self._length = 0 # 4 bytes, body length
        self._ip = 0 # 4 bytes

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

            BusinessConnection.clients_lock.acquire()
            self._function = function
            if function in BusinessConnection.clients:
                BusinessConnection.clients[function].add(self)
            else:
                new_business = set()
                new_business.add(self)
                BusinessConnection.clients[function] = new_business
            self._registed = True 
            BusinessConnection.clients_lock.release()

        reply_str = json.dumps(reply)
        header = pack("I", socket.htonl(len(reply_str)))
        self._stream.write(header + reply_str)


    def send(self, msg, device_type=1, ip_str='127.0.0.1'):
        from socket import htonl
        device_id = 1 # id is unuseful
        timestamp = time.time()
        ip = unpack("I", socket.inet_aton(ip_str))[0]
        import hashlib
        verify = hashlib.md5()
        verify.update(msg)
        md5 = verify.hexdigest()
        length = len(msg)
        
        header = pack("2I32sdII", htonl(device_type),
            htonl(device_id), md5, timestamp,
            htonl(length), htonl(ip))
        logging.debug('send header:(%d, %d, %s, %.4f, %d, %s) to %s'
            % (device_type, device_id, md5, timestamp,
               length, ip_str, self._addr_str))

        self._stream.write(header + msg)
        logging.debug('send msg:%s to %s' % (msg, self._addr_str))
        self.read_header()


    def read_header(self):
        self._stream.read_bytes(BusinessConnection.header_length, self.read_body)


    def read_body(self, header):
        self._header = header
        parts = unpack("2I32sdII", self._header)
        from socket import ntohl

        (self._type, self._id, self._md5,
            self._timestamp, self._length, self._ip) = parts

        self._type = ntohl(self._type)
        self._id = ntohl(self._id)
        self._length = ntohl(self._length)
        self._ip = socket.inet_ntoa(pack('I', ntohl(self._ip)))

        logging.debug('read header(%d, %d, %s, %f, %d, %s) from %s' % (
            self._type, self._id, self._md5,
            self._timestamp, self._length, self._ip,
            self._addr_str))

        self._stream.read_bytes(self._length, self.parse_main)


    def parse_main(self, body):
        logging.debug('read body(%d:%s) from %s' % (len(body),
            body, self._addr_str))
        self._body = body


    def on_close(self):
        self._stream.close()
        if self._registed:
            BusinessConnection.clients_lock.acquire()
            BusinessConnection.clients[self._function].remove(self)
            if len(BusinessConnection.clients[self._function]) == 0:
                BusinessConnection.clients.pop(self._function)
            BusinessConnection.clients_lock.release()
            logging.info('function %s disconnected from %s' % (self._function, self._addr_str))


