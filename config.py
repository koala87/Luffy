#!/usr/bin/env python2.7
#coding=utf-8

"""config"""

__author__ = "Yingqi Jin <jinyingqi@luoha.com>"

__all__ = ['get_server', 'get_server_intro', 'read_config']

import os
import ConfigParser


class Singleton(object):
    def __new__(cls, *args, **kw):
        if not hasattr(cls, "_instance"):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args, **kw)
        return cls._instance


class Configure(Singleton):

    def __init__(self):
        self.server_intro_map = {}
        self.request_server_map = {}
        self.request_function_map = {}

        self.read_config()


    def read_config(self, fname='route.conf'):
        if not os.path.exists(fname):
            logging.error('config %s does not exsit' % fname)
            return
        cf = ConfigParser.ConfigParser()
        cf.read(fname)
        secs = cf.sections()
        
        if 'server' in secs:
            opts = cf.options('server')
            for opt in opts:
                str_val = cf.get('server', opt)
                self.server_intro_map[opt] = str_val

        for sev in self.server_intro_map.iterkeys():
            if sev in secs:
                opts = cf.options(sev)
                for opt in opts:
                    str_val = cf.get(sev, opt)
                    request = int(opt)
                    self.request_function_map[request] = str_val
                    self.request_server_map[request] = sev


    def get_all_server(self):
        return self.server_addr_map


    def get_server(self, request):
        return self.request_server_map.get(request, None)


    def get_server_intro(self, server):
        return self.server_intro_map.get(server, None)


__configure = Configure()


def get_all_server():
    return __configure.get_all_server()


def get_server(request):
    return __configure.get_server(request)


def get_server_intro(server):
    return __configure.get_server_intro(server)


def read_config(fname):
    __configure.read_config(fname)


if __name__ == '__main__':

    print get_server(10001)
    print get_server_intro('control')
