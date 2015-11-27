#!/usr/bin/env python
#!coding=utf-8

import sys
import time 
import json
import socket
import signal
import logging
import threading
from struct import pack, unpack

STOP = False
THREADS = []

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
        default=10, help="specify threads num, default is 10")
    parser.add_option("-l", "--log", dest="log",
        default=get_default_log(), help="specify log name")
    parser.add_option("-d", "--debug", dest="debug",
        action='store_true',
        default=False, help="enable debug")

    (options, args) = parser.parse_args() 
    return options



def stop_threads():
    for th in THREADS:
        th.stop()
    global STOP
    STOP = True


def sig_handler(sig, frame):
    stop_threads()


class Client(threading.Thread):
    clients = set()
    def __init__(self, ip, port):
        Client.clients.add(self)
        threading.Thread.__init__(self)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._address = (ip, port)
        self.thread_stop = False
        logging.info('new connection %d to %s:%d' % (len(Client.clients), self._address[0], self._address[1]))

    def run(self):
        try:
            self._sock.connect(self._address)
        except socket.error, arg:
            (errno, err_msg) = arg
            logging.error('connect server failed: %s, errno=%d' % (err_msg, errno))
            return
        
        self.send_register()


    def send_register(self):
        """send json register info
        header : 4 bytes length of body, 32 bytes md5
        body: json string
            function, timestamp
        """
        import hashlib
        
        body = {}
        body['function'] = 'control'
        body['timestamp'] = time.time()
        msg = json.dumps(body)

        verify = hashlib.md5()
        verify.update(msg)
        md5 = verify.hexdigest()

        msg = json.dumps(body)
        header = pack("I32s", socket.htonl(len(msg)), md5)
        self._sock.send(header + msg)
        logging.debug('send register info: header: %d, %s body:%s' % (len(msg), md5, msg))

        self.read_register_feedback()


    def read_register_feedback(self):
        """read server feedback
        header : 4 bytes length of body
        body : status 
        """
        msg = self._sock.recv(1024)
        header = msg[:4]
        length = socket.ntohl(unpack('I', header)[0])
        if len(msg) < length + 4:
            logging.error('register feedback length is less than %d' % length + 4)
            return
        body = json.loads(msg[4:4+length])
        if 'status' not in body:
            logging.error('status field not in body')
            return
        status = body['status'] 
        if status == 0:
            logging.info('register successfully : header:%d body:%s' % (length, msg[4:4+length]))
        else:
            logging.info('register failed')


    def send(self):
        body = 'hello world'
        orig = [17, 100, 10018, 65536, len(body), 520]
        elems = [socket.htonl(x) for x in orig]
        header = pack('6I', elems[0], elems[1], elems[2],
                            elems[3], elems[4], elems[5])
        msg = header + body
        try:
            self._sock.send(msg)
        except socket.error, arg:
            (errno, err_msg) = arg
            logging.error('send msg to server failed: %s, errno=%d' % (err_msg, errno))
            stop_threads()
            return
        
        header_str = ', '.join([str(x) for x in orig])
        logging.debug('send header: (%d : %s) to %s:%d' % (len(header), header_str,
            self._address[0], self._address[1]))
        logging.debug('send body: (%d : %s) to %s:%d' % (len(body), body, 
            self._address[0], self._address[1]))


    def stop(self):
        self.thread_stop = True


if __name__ == '__main__':

    opts = register_options()

    init_log(opts.log, opts.debug)

    logging.info('start %d threads to server %s:%d ...' % (opts.num, opts.host, opts.port))

    for i in xrange(opts.num):
        client = Client(opts.host, opts.port)
        THREADS.append(client)
    
    for i in THREADS: 
        i.start()

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    # master thread to catch signal
    while not STOP:
        time.sleep(0.01)

    logging.info('stop ...')

