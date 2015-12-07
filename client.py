#!/usr/bin/env python
#!coding=utf-8

import sys
import time 
import socket
import signal
import logging
from threading import Thread
from struct import pack, unpack

import tornado.iostream
import tornado.ioloop

THREADS = []

def init_log():
    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s - %(process)-6d - %(threadName)-10s - %(levelname)-8s]\t%(message)s',
        datefmt='%a, %d %b %Y %H:%M:%S',
        filename='log/client.log',
        filemode='w')
    
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)-8s %(message)s')
    sh.setFormatter(formatter)
    logging.getLogger('').addHandler(sh)


def get_ip():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip


def register_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-i", "--host", dest="host",
        default=get_ip(), help="specify host, default is localhost")
    parser.add_option("-p", "--port", dest="port",
        type="int",
        default=58849, help="specify port, default is 58849")
    parser.add_option("-n", "--num", dest="num",
        type="int",
        default=1, help="specify threads num, default is 1")

    (options, args) = parser.parse_args() 
    return options


def sig_handler(sig, iframe):
    tornado.ioloop.IOLoop.current().stop()
    for i in THREADS:
        i.stop()
    logging.info('stop ...')


class Client(Thread):
    clients = set()
    def __init__(self, ip, port):
        Thread.__init__(self) 
        Client.clients.add(self)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._address = (ip, port)
        self._addr_str = ip + ':' + str(port)

        self._stream = ''
        self._stop = False


    def run(self):
        while not self._stop:
            try:
                self._sock.connect(self._address)
                logging.info('new client connection %d to %s' %
                    (len(Client.clients), self._addr_str))
                break
            except socket.error, arg:
                (errno, err_msg) = arg
                logging.error('connect to server %s failed: %s, errno=%d' %
                    (self._addr_str, err_msg, errno))
            time.sleep(1)

        self._stream = tornado.iostream.IOStream(self._sock)
        self._stream.set_close_callback(self.on_close)
        
        self._stream.read_bytes(24, self.read_header)

        while not self._stop:
            self.send()
            time.sleep(0.1)


    def stop(self):
        self._stop = True


    def read_header(self, header):
        from socket import ntohl
        # extract route header
        parts = unpack("6I", header)
        parts = [ntohl(x) for x in parts]
        
        author, version, request, verify, length, device_id = parts

        logging.debug('read header:(%d, %d, %d, %d, %d, %d)'
            % (author, version, request,
               verify, length, device_id))

        self._stream.read_bytes(length, self.read_body)


    def read_body(self, body):
        logging.debug('read body: %s' % body)
 

    def send(self):
        body = 'hello world'
        orig = [17, 100, 10001, 65536, len(body), 520]
        elems = [socket.htonl(x) for x in orig]
        header = pack('6I', elems[0], elems[1], elems[2],
                            elems[3], elems[4], elems[5])
        msg = header + body
        try:
            self._stream.write(msg)
        except socket.error, arg:
            (errno, err_msg) = arg
            logging.error('send msg to server failed: %s, errno=%d' % (err_msg, errno))
            return
        
        header_str = ', '.join([str(x) for x in orig])
        logging.debug('send header: (%d : %s) to %s:%d' % (len(header), header_str,
            self._address[0], self._address[1]))
        logging.debug('send body: (%d : %s) to %s:%d' % (len(body), body, 
            self._address[0], self._address[1]))
            

    def on_close(self):
        logging.debug('disconnected from %s:%d' % (len(header), header_str))
        self._stream.close()


if __name__ == '__main__':

    init_log()

    opts = register_options()

    logging.info('start %d threads to server %s:%d ...' % (opts.num, opts.host, opts.port))

    for i in xrange(opts.num):
        cli = Client(opts.host, opts.port)
        THREADS.append(cli)

    for i in THREADS:
        i.setDaemon(True)
        i.start()

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    tornado.ioloop.IOLoop.current().start()

    logging.info('stop ...')

