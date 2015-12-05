#!/usr/bin/env python
#!coding=utf-8

__author__ = 'Yingqi Jin <jinyingqi@luoha.com>'

__all__ = ['Client.send_register', 'Client.read_register_feedback',
           'Client.read_packet', 'Client.send_packet']

import sys
import time 
import json
import socket
import signal
import logging
import threading
from struct import pack, unpack
import tornado.iostream
import tornado.ioloop


# packet header length between route and business
BUSINESS_HEADER_LENGTH = 56
# packet header length between app/box/erp/init and business
CLIENT_HEADER_LENGTH = 24


def init_log(fname, debug):
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
    """return default log name"""
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
        default=6666, help="specify port, default is 3050")
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



class BaseBusiness(object):
    clients = set()
    function = 'control'

    def __init__(self, ip, port):
        BaseBusiness.clients.add(self)

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._address = (ip, port)
        self.thread_stop = False

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

        logging.info('new connection %d to %s:%d' % (len(BaseBusiness.clients), self._address[0], self._address[1]))

        self.run()


    def run(self):
        try:
            self._sock.connect(self._address)
        except socket.error, arg:
            (errno, err_msg) = arg
            logging.error('connect server failed: %s, errno=%d' % (err_msg, errno))
            return
        self._stream = tornado.iostream.IOStream(self._sock)
        self._stream.set_close_callback(self.on_close)
        
        self.send_register()


    def send_register(self):
        """send json register info
        header : 4 bytes length of body, 32 bytes md5
        body: json string
            function, timestamp
        """
        import hashlib
        
        body = {}
        body['function'] = BaseBusiness.function 
        body['timestamp'] = time.time()
        msg = json.dumps(body)

        verify = hashlib.md5()
        verify.update(msg)
        md5 = verify.hexdigest()

        msg = json.dumps(body)
        header = pack("I32s", socket.htonl(len(msg)), md5)
        self._stream.write(header + msg)
        logging.debug('send register info: header: %d, %s body:%s' % (len(msg), md5, msg))

        self._stream.read_bytes(4, self.read_register_feedback_header)


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


        self._stream.read_bytes(BUSINESS_HEADER_LENGTH, self.read_packet_header)


    def read_packet_header(self, header):
        from socket import ntohl
        # extract route header
        self._header = header 
        parts = unpack("2I32sdII", self._header)
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
        self.do_something()

        # send result back
        self.send_packet_back()


    #!!! import logic handler
    def do_something(self):
        logging.debug('do something ...')

        

    def send_packet_back(self):
        from socket import htonl
        type_map = {
            1 : 'app',
            2 : 'box',
            3 : 'erp',
            4 : 'init',
        }
        cli = 'no type'
        if self._device_type in type_map:
            cli = type_map[self._device_type]
        
        #!!! put business logic result here

        body = 'hi~ %s' % cli

        ip = htonl(unpack('I', socket.inet_aton(self._ip))[0])
        header = pack("2I32sdII", htonl(self._device_type),
            htonl(self._device_id), self._md5, self._timestamp,
            htonl(len(body)), ip)

        msg = header + body
        self._stream.write(msg)
        logging.debug('send packet back: header(%d, %d, %s, %.4f, %d, %s) body:%s'
            % (self._device_type, self._device_id, self._md5,
               self._timestamp, len(body), self._ip, body))

        self._stream.read_bytes(BUSINESS_HEADER_LENGTH, self.read_packet_header)


    def on_close(self):
        logging.debug('%s business disconnected' % BaseBusiness.function)



if __name__ == '__main__':

    opts = register_options()

    init_log(opts.log, opts.debug)

    logging.info('start %d threads to server %s:%d ...' % (opts.num, opts.host, opts.port))

    for i in xrange(opts.num):
        BaseBusiness(opts.host, opts.port)
    
    tornado.ioloop.IOLoop.current().start()

    logging.info('stop ...')

