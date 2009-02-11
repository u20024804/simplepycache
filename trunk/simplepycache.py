#! /usr/bin/env python
#coding=utf-8

from __future__ import with_statement
#import stackless
from threading import Thread, Lock
from multiprocessing.connection import Listener, Client
from time import time, sleep
import pyprocps


RESERVED_MEM = 200*1000
CLEAN_CYCLE = 10.0
CLEAN_COST = 1.0
LIVE_TIME = 10.0


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
        self._lock = Lock()
        self._data = {}
        self._lastModify = {}
        self._lastVisited = time()
        Thread.__init__(self)
        self.daemon = deamon
        
    def config(self, address, deamon=None):
        self._address = address
        if deamon is not None:
            self.daemon = deamon
        
    def run(self):
        listen = Listener(self._address)
        print 'simplepycache server start to listen at', self._address
        cleaner = self.Cleaner(self)
        cleaner.deamon = True
        cleaner.start()        
        while True:
            conn = listen.accept()
            self.Worker(self, conn).start()
            
    class Worker(Thread):
        def __init__(self, server, conn):
            Thread.__init__(self)
            self._server = server
            self._conn = conn
            
        def _process(self, opcode, *args):
            key = args[0]
            _data = self._server._data
            lastModify = self._server._lastModify
            now = time()
            self._server._lastVisited = now
            if opcode == IN:
                isIn = key in _data
                if isIn:
                    lastModify[key] = now
                return isIn
            elif opcode == READ:
                value = _data[key]
                lastModify[key] = now
                return value
            elif opcode == WRITE:
                with self._server._lock:
                    _data[key] = args[1]
                    lastModify[key] = now
            elif opcode == REMOVE:
                with self._server._lock:
                    del _data[key]
                    del lastModify[key]
            else:
                raise BadOpcode()
        
        def run(self):
            lock = self._server._lock
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
                    print type(ex), ex, type(ret), ret
                    return
    
    class Cleaner(Thread):
        def __init__(self, server, cycle=CLEAN_CYCLE, reserved_mem=RESERVED_MEM,
                        clean_cost=CLEAN_COST, live_time=LIVE_TIME):
            Thread.__init__(self)
            self._proxy = CacheClient(server._address)
            self._server = server
            self._cycle = cycle
            self._reservedMem = reserved_mem
            self._cleanCost = clean_cost
            self._liveTime = live_time
            self._avgSize = 50
            self._evolve = 3.0
            
        def run(self):
            proxy = self._proxy
            data = self._server._data
            lastModify = self._server._lastModify
            lock = self._server._lock
            cycle = self._cycle
            reservedMem = self._reservedMem
            cleanCost = self._cleanCost
            liveTime = self._liveTime
            avgSize = self._avgSize
            evolve = self._evolve
            while True:
                sleep(cycle)
                size = reservedMem - allFree()
                total = size / avgSize
                plan2count = total
                cost = total / cleanCost / 10000
                if size <= 0:
                    continue
                count = 0
                start = time()
                
                iterTime = 0
                while True:
                    iterTime += 1
                    distance = liveTime * total / 10000
                    now = time()
                    toRemove = []
                    with lock:
                        for key in data:
                            if now - lastModify[key] >= distance:
                                toRemove.append(key)
                    for key in toRemove:
                        try:
                            del data[key]
                            del lastModify[key]
                            count += 1
                        except:
                            pass                    
                    size = reservedMem - allFree()
                    if size <= 0:
                        break
                    liveTime -= liveTime / evolve
                    total = size / avgSize
                end = time()
                realCost = 10000 * (end - start) / count
                cleanCost += (realCost - cleanCost) / evolve
                avgSize += (count - plan2count) / plan2count / evolve
                
class BadOpcode(Exception):
    pass


def allFree():
    meminfo = pyprocps.meminfo()
    return int(meminfo['Cached']) + int(meminfo['Buffers']) + int(meminfo['MemFree'])


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
            r = self._conn.recv()
            (ret, Exception, ex,) = r
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
