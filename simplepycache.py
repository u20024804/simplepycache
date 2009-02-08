#! /usr/bin/env python
#coding=utf-8

from __future__ import with_statement
#import stackless
from threading import Thread, Lock
from multiprocessing.connection import Listener, Client


IN = 0
READ = 1
WRITE = 2
REMOVE = 3
LOCK = 6
UNLOCK = 7


DEFAULTADDRESS = ('127.0.0.1', 8698,)

class CacheServer(Thread):
    def __init__(self, address=DEFAULTADDRESS, deamon=True):
        self._address = address
        self._dict = {}
        Thread.__init__(self)
        self.daemon = deamon
        
    def config(self, address, deamon=None):
        self._address = address
        if deamon is not None:
            self.daemon = deamon
        
    def run(self):
        listen = Listener(self._address)
        print 'simplepycache server start to listen at', self._address
        while True:
            conn = listen.accept()
            self.Worker(self, conn).start()
            
    class Worker(Thread):
        def __init__(self, server, conn):
            Thread.__init__(self)
            self._server = server
            self._conn = conn
            self._dict = self._server._dict
            
        def _process(self, opcode, *args):
            _dict = self._dict
            if opcode == IN:
                return args[0] in _dict
            elif opcode == READ:
                return _dict[args[0]]
            elif opcode == WRITE:
                _dict[args[0]] = args[1]
                return
            elif opcode == REMOVE:
                del _dict[args[0]]
                return
            else:
                raise BadOpcode()
        
        def run(self):
            while True:
                conn = self._conn
                try:
                    args = conn.recv()
                except Exception, ex:
                    try:
                        conn.close()
                    except Exception, ex:
                        print type(ex), ex
                    return
                try:
                    ret = (self._process(*args), None, None,)
                except Exception, ex:
                    ret = (None, type(ex), ex)
                try:
                    conn.send(ret)
                except IOError:
                    pass
                except Exception, ex:
                    print type(ex), ex
                    return
                
class BadOpcode(Exception):
    pass


class CacheClient(object):
    def __init__(self, address=DEFAULTADDRESS):
        self._address = address
        self._conn = Client(self._address)
        self._lock = Lock()
        
    def close(self):
        with self._lock:
            self._conn.close()
            
    def __del__(self):
        try:
            self.close()
            pass
        except:
            pass
        
    def _rpc(self, *args):
        with self._lock:
            self._conn.send(args)
            (ret, Exception, ex,) = self._conn.recv()
            if Exception or ex:
                raise Exception, ex
            else:
                 return ret        
        
    def __contains__(self, key):
        return self._rpc(IN, key)
    
    def __getitem__(self, key):
        return self._rpc(READ, key)
    
    def __setitem__(self, key, value):
        return self._rpc(WRITE, key, value)
    
    def __delitem__(self, key):
        return self._rpc(REMOVE, key)
    

def waitForDie():
    while True:
        try:
            raw_input()
        except KeyboardInterrupt:
            return


if __name__ == '__main__':
    import sys
    cacheserver = CacheServer()
    if len(sys.argv) == 2:
        address = sys.argv[1]
        cacheserver.config(address)
    if len(sys.argv) == 3:
        address = (sys.argv[1], int(sys.argv[2]))
        cacheserver.config(address)
    cacheserver.start()
    waitForDie()
