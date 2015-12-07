#coding=utf-8

"""utility routines"""

import os
import sys
import socket
import logging

# business register info header length. 4 bytes len + 32 bytes md5
BUSINESS_REGISTER_HEADER_LENGTH = 36

# business register feedback header length. 4 bytes len
BUSINESS_REGISTER_FEEDBACK_HEADER_LENGTH = 4

BUSINESS_HEADER_LENGTH = 56

BUSINESS_FEEDBACK_HEADER_LENGTH = 4 

# app/box/erp/init packet header length 
CLIENT_HEADER_LENGTH = 24

ROOT = os.path.dirname(os.path.abspath(__file__))

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
    name = os.path.basename(sys.argv[0])
    pos = name.rfind('.')
    if pos != -1:
        name = name[:pos]
    name = os.path.abspath(os.path.join(ROOT, '../log/%s.log' % name))
    return name


def get_ip():
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip


if __name__ == '__main__':
    
    print get_ip()
