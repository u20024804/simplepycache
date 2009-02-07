#! /usr/bin/env python
#coding=utf-8

from threading import Thread


recv = object()
send = object()

class Recv(object):
    def get(self): return self._value
    def set(self, value): self._value = value

class MySL(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._tasks = []
        self._msgs = {}
        self._recvers = {}
        self._next = None
        self._size = 0

    def task(self, task, *fargs, **fkwargs):
        target  =  lambda : task(*fargs, **fkwargs) if fargs or fkwargs else task
        p = self._size
        self._tasks.append([target, False,])
        self._recvers[target] = None
        self._msgs[target] = []
        self._size += 1
        if p == 0:
            self._next = target, 0
        return (self, target, p,)
        
    def _wakeup(self, p):
        self._tasks[p][1] = False

    def _pend(self, p):
        self._tasks[p][1] = True
        
    def _done(self, p):
        del self._tasks[p]
        self._size -= 1
        if self._size == self._next[1]:
            self._next = (None, 0)
            self._moveNext()
        
    def _dealmsg(self, task):
        owner, src, tp, msg = self._msgs[task].pop(0)
        self._wakeup(tp)
        recver = self._recvers[task]
        self._recvers[task] = None
        recver.set(msg)

    def _moveNext(self):
        p = self._next[1]
        for i in xrange(self._size):
            p += 1
            if p >= self._size: p = p % self._size 
            if self._tasks[p][1] == False:
                self._next = self._tasks[p][0], p
                return
        self._next = None

    def _fetch(self):
        r = self._next
        if r is not None:
            self._moveNext()
        return r

    def _deliver(self, p):
        self._tasks[p][1] = False

    def _recv(self, task, p, recver):
        if not isinstance(recver, Recv):
            raise self.BadYield('the receiver must be an instance of the class Recv')
        self._recvers[task] = recver                        
        if task in self._msgs and len(self._msgs[task]):
            self._deliver(task)
        else:
            self._pend(p)

    def _send(self, task, p, dest, msg):
        self._pend(p)
        owner, target, tp = dest
        if target not in self._msgs:
            self._msgs[target] = []
        self._msgs[tp].append((self, task, p, msg,))
        self._wakeup(tp)

    def run(self):
        while True:
            task = self._fetch()
            if task is None:
                break
            task, p = task
            try:
                if callable(task):
                    task()
                    self._done(p)
                else:
                    if self._recvers[task] and self._msgs[task]:
                        self._dealmsg(task)
                    nextop = task.next()
                    if nextop is None:
                        self._done(p)
                    elif len(nextop) == 2 and nextop[0] == recv:
                        self._recv(task, p, nextop[1])
                    elif len(nextop) == 3 and nextop[0] == send:
                        self._send(task, p, nextop[1], nextop[2])
                    else:
                        raise self.BadYield() 
            except Exception, ex:
                if not isinstance(ex, StopIteration):
                    print type(ex), ex

        for task in self._tasks:
            if task[1] == True:
                raise self.DeadLock('There is no sender but receiver or no receiver but sender!')
        
    class BadYield(Exception):
        pass
    class DeadLock(Exception):
        pass

def task0(x):
    print x

def task1(x):
    recver = Recv()
    while True:
        #print 'recv...', x
        yield recv, recver
        r = recver.get()
        if r is not None:
            #print 'recv', x, r
            pass
        else:
            print 'recv end'
            yield
    yield

def task2(x, target):
    for i in xrange(1000000):
        #print 'send', target, x
        yield send, target, x
    #print 'task2 end:', x
    yield

def task3(target):
    for i in xrange(1000000):
        #print 'send', target
        yield send, target, i
    #print 'send end'
    yield send, target, None

def test():
    mysl = MySL()
    #mysl.task(task0, 1)
    #mysl.task(task0, 2)
    t2 = mysl.task(task1(1))
    for i in xrange(10000):
        mysl.task(task2(i, t2))
    mysl.task(task3(t2))
    #mysl.start()
    #mysl.join()
    mysl.run()

if __name__ == '__main__':
    from time import time
    t1 = time()
    test()
    t2 = time()
    print t2 - t1
