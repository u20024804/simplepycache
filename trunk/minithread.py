#! /usr/bin/env python
#coding=utf-8

try:
    import psyco
    psyco.full()
except ImportError:
    print 'Psyco not installed, the program will just run slower'

from collections import deque
from threading import Thread


recv = object()
send = object()

class Recv(object):
    def get(self): return self._value
    def set(self, value): self._value = value

class MiniThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._tasks = deque()
        self._channels = deque()
        self._times1 = 0
        self._times2 = 0


    class Task(object):
        def __init__(self, thread, target):
            self.thread = thread
            self.target = target
            self.recvor = None

    class Channel(object):
        def __init__(self, thread):
            self.thread = thread
            self.recvers = deque()
            self.msgs = deque()
            
    class Msg(object):
        def __init__(self, source, content):
            self.source = source
            self.content = content

    def task(self, target, *fargs, **fkwargs):
        if fargs or fkwargs or callable(target):
            _target  =  lambda : target(*fargs, **fkwargs)
        else:
            _target = target
        task = self.Task(self, _target)
        self._tasks.append(task)
    
    def channel(self):
        channel = self.Channel(self)
        self._channels.append(channel)
        return channel

    def _done(self, _task):
        pass

    def _fetch(self):
        try:
            return self._tasks.popleft()
        except IndexError:
            return None

    def _recv(self, task, channel, recvor):
        self._times2 += 1
        if not isinstance(recvor, Recv):
            raise BadYield('the receiver must be an instance of the class Recv')
        if channel.msgs:
            msg = channel.msgs.popleft()
            recvor.set(msg.content)
            self._tasks.append(msg.source)
            self._tasks.append(task)
        else:
            task.recvor = recvor
            channel.recvers.append(task)
        
    def _send(self, task, channel, content):
        self._times1 += 1
        msg = self.Msg(task, content)
        if channel.recvers:
            recver = channel.recvers.popleft()
            recver.recvor.set(msg.content)
            recver.recvor = None
            self._tasks.append(recver)            
            self._tasks.append(task)
        else:
            channel.msgs.append(msg)

    def run(self):
        while True:
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
                        raise BadYield()
            except BadYield, by:
                raise by
            except StopIteration:
                self._done(task)
            except Exception, ex:
                print type(ex), ex

        print 'run times:', self._times1, self._times2
        for channel in self._channels:
            if channel.recvers or channel.msgs:
                raise DeadLock('There is no sender but receiver or no receiver but sender!')

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
    for i in xrange(1):
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
    for i in xrange(100000):
        mini.task(task2(channel, i))
    #mini.task(task3(channel))
    #mysl.start()
    #mysl.join()
    mini.run()

if __name__ == '__main__':
    from time import time
    t1 = time()
    try:
        test()
    except DeadLock:
        pass
    t2 = time()
    print t2 - t1
