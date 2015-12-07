#coding=utf-8
"""basic business template: it supports Asynchronous IO using tornado ioloop/iostream
subclass inherits Business class and overwrite the routinue process
eg.
#control.py
class Control(Business):
    control_clients = set()
    def __init__(self, ip='localhost', port=58849):
        Business.__init__('control', ip, port)

    def process(self):
        #TODO
        pass
"""

__author__ = 'Yingqi Jin <jinyingqi@luoha.com>'

import sys
import time 
import json
import socket
import logging
from struct import pack, unpack
import tornado.iostream
import tornado.ioloop

from utility import BUSINESS_REGISTER_HEADER_LENGTH, BUSINESS_REGISTER_FEEDBACK_HEADER_LENGTH
from utility import BUSINESS_HEADER_LENGTH, BUSINESS_FEEDBACK_HEADER_LENGTH, CLIENT_HEADER_LENGTH


class Business(object):
    clients = set()

    def __init__(self, function='control', ip='localhost', port=58849):
        Business.clients.add(self)

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._ip = ip
        self._port = port
        self._addr_str = ip + ':' + str(port)
        self._address = (ip, port)
        self._function = function

        # route server packet header
        self._header_length = BUSINESS_HEADER_LENGTH
        self._device_type = 0
        self._device_id = 0
        self._md5 = ''
        self._timestamp = 0
        self._length = 0
        self._ip = 0

        # client packet header
        self._client_header_length = CLIENT_HEADER_LENGTH
        self._author = 0
        self._version = 0
        self._request = 0
        self._verify = 0
        self._length = 0
        self._device = 0

        self.connect()


    def connect(self):
        try:
            self._sock.connect(self._address)
            logging.info('%s module connect to router %s successfully' %
                (self._function, self._addr_str))
            self._stream = tornado.iostream.IOStream(self._sock)
            self._stream.set_close_callback(self.on_close)
        
            self.send_register()
        except socket.error, arg:
            (errno, err_msg) = arg
            logging.error('%s module connect to router %s failed: %s:%d' %
                (self._function, self._addr_str, err_msg, errno))
            self.on_close()


    def send_register(self):
        """send json register info
        header : 4 bytes length of body, 32 bytes md5
        body: json string
            function, timestamp
        """
        import hashlib
        
        body = {}
        body['function'] = self._function 
        body['timestamp'] = time.time()
        msg = json.dumps(body)

        verify = hashlib.md5()
        verify.update(msg)
        md5 = verify.hexdigest()

        msg = json.dumps(body)
        header = pack("I32s", socket.htonl(len(msg)), md5)
        self._stream.write(header + msg)
        logging.debug('send register info: header: %d, %s body:%s' % (len(msg), md5, msg))

        self._stream.read_bytes(
            BUSINESS_REGISTER_FEEDBACK_HEADER_LENGTH,
            self.read_register_feedback_header)


    def read_register_feedback_header(self, header):
        self._length = socket.ntohl(unpack('I', header)[0])
        self._stream.read_bytes(self._length, self.read_register_feedback_body)


    def read_register_feedback_body(self, body_str):
        body = json.loads(body_str)
        if 'status' not in body:
            logging.error('status field not in body')
            return
        status = body['status'] 

        if status == 0:
            logging.info('register successfully : header:%d body:%s' % (self._length, body))
        else:
            logging.info('register failed')
            self.on_close()

        self._stream.read_bytes(BUSINESS_HEADER_LENGTH, self.read_packet_header)


    def read_packet_header(self, header):
        from socket import ntohl
        # extract route header
        self._header = header 
        parts = unpack("II32sdII", self._header)
        (self._device_type, self._device_id, self._md5,
            self._timestamp, self._length, self._ip) = parts

        # convert integers from network to host byte order
        self._device_type = ntohl(self._device_type)
        self._device_id = ntohl(self._device_id)
        self._length = ntohl(self._length)
        self._ip = socket.inet_ntoa(pack('I', ntohl(self._ip)))

        logging.debug('read header:(%d, %d, %s, %.4f, %d, %s)'
            % (self._device_type, self._device_id, self._md5,
               self._timestamp, self._length, self._ip))

        self._stream.read_bytes(self._length, self.read_packet_body)


    def read_packet_body(self, body):
        from socket import ntohl
        self._body = body 

        # unpack body header
        parts = unpack('6I', self._body[:self._client_header_length])
        parts = [ntohl(x) for x in parts] 
        (self._author, self._version, self._request,
            self._verify, self._length, self._device) = parts
 
        body1 = self._body[self._client_header_length:]
        logging.debug('read body: header(%d, %d, %d, %d, %d, %d) body:%s'
            % (self._author, self._version, self._request,
               self._verify, self._length, self._device, body1))

        # all logic will be handled here
        self.process_packet()

        # read next packets
        self._stream.read_bytes(BUSINESS_HEADER_LENGTH, self.read_packet_header)


    # child class overload this routine
    def process_packet(self):
        logging.debug('process packet ...')

        self.send()


    def send(self, body='hi~'):
        """send packet back"""
        from socket import htonl
        ip = htonl(unpack('I', socket.inet_aton(self._ip))[0])
        header = pack("2I32sdII", htonl(self._device_type),
            htonl(self._device_id), self._md5, self._timestamp,
            htonl(len(body)), ip)

        msg = header + body
        self._stream.write(msg)
        logging.debug('send packet back: header(%d, %d, %s, %.4f, %d, %s) body:%s'
            % (self._device_type, self._device_id, self._md5,
               self._timestamp, len(body), self._ip, body))
        

    def on_close(self):
        logging.debug('%s module disconnected' % self._function)
