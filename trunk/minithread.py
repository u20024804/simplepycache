#! /usr/bin/env python
#coding=utf-8

from threading import Thread


recv = object()
send = object()

class Recv(object):
    def get(self): return self._value
    def set(self, value): self._value = value

class MiniThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._tasks = []
        self._channels = []
        self._csize = 0
        self._current = -1
        self._size = 0
        #self._times = 0


    class Task(object):
        def __init__(self, thread, target, p):
            self.thread = thread
            self.target = target
            self.p = p
            self.awake = True
            self.channel = None
            
    end = Task(None, None, -1)
            
    class Channel(object):
        def __init__(self, thread, p):
            self.thread = thread
            self.p = p
            self.recvers = []
            self.msgs = []
            
    class Msg(object):
        def __init__(self, source, content):
            self.source = source
            self.content = content

    def task(self, target, *fargs, **fkwargs):
        if fargs or fkwargs or callable(target):
            _target  =  lambda : target(*fargs, **fkwargs)
        else:
            _target = target
        p = self._size
        task = self.Task(self, _target, p)
        self._tasks.append(task)
        self._size += 1
    
    def channel(self):
        p = self._csize
        c = self.Channel(self, p)
        self._channels.append(c)
        self._csize += 1
        return c
        
    def _wakeup(self, task):
        task.awake = True

    def _pend(self, task):
        task.awake = False
        
    def _done(self, task):
        del self._tasks[task.p]
        self._current -= 1
        self._size -= 1
        
    def _dealmsg(self, task):
        msg = task.channel.msgs.pop(0)
        self._wakeup(msg.source)
        recver = task.channel.recvers.pop(0)
        recver.set(msg.content)
        self._wakeup(task)

    def _fetch(self):
        p = self._current
        for i in xrange(self._size):
            p += 1
            if p >= self._size: p = p % self._size 
            task = self._tasks[p]
            if task.channel and task.channel.recvers and task.channel.msgs:
                self._dealmsg(task)
            if task.awake:
                self._current = p
                task.p = p
                return task
        self._current = -1
        return None

    def _recv(self, task, channel, recver):
        if not isinstance(recver, Recv):
            raise self.BadYield('the receiver must be an instance of the class Recv')
        channel.recvers.append(recver)
        task.channel = channel
        self._pend(task)

    def _send(self, task, channel, msg):
        task.channel = None
        self._pend(task)
        channel.msgs.append(self.Msg(task, msg))

    def run(self):
        while True:
            #self._times += 1
            task = self._fetch()
            if task is None:
                break
            try:
                target = task.target
                if callable(target):
                    target()
                    self._done(task)
                else:
                    nextop = target.next()
                    if nextop is None:
                        self._done(task)
                    elif len(nextop) == 3 and nextop[0] == recv:
                        self._recv(task, nextop[1], nextop[2])
                    elif len(nextop) == 3 and nextop[0] == send:
                        self._send(task, nextop[1], nextop[2])
                    else:
                        raise self.BadYield()
            except self.BadYield, by:
                raise by
            except StopIteration:
                self._done(task)
            except Exception, ex:
                print type(ex), ex

        if self._size:
            raise self.DeadLock('There is no sender but receiver or no receiver but sender!')
        #print 'run times:', self._times
        
    class BadYield(Exception):
        pass
    class DeadLock(Exception):
        pass

def task0(x):
    print 'task0', x

def task1(channel, x):
    recver = Recv()
    while True:
        #print 'recv...', x
        yield recv, channel, recver
        r = recver.get()
        if r is not None:
            #print 'recv', x, r
            pass
        else:
            #print 'recv end'
            yield
    yield

def task2(channel, x):
    for i in xrange(10):
        #print 'send', channel, x, 'task2'
        yield send, channel, x
    #print 'task2 end:', x
    yield

def task3(channel):
    for i in xrange(10):
        #print 'send', channel, 'task3'
        yield send, channel, i
    #print 'send end'
    yield send, channel, None

def test():
    mini = MiniThread()
    #mini.task(task0, 1)
    #mini.task(task0, 2)
    channel = mini.channel()
    mini.task(task1(channel, -1))
    for i in xrange(1000):
        mini.task(task2(channel, i))
    mini.task(task3(channel))
    #mysl.start()
    #mysl.join()
    mini.run()

if __name__ == '__main__':
    from time import time
    t1 = time()
    test()
    t2 = time()
    print t2 - t1
