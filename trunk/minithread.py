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
        self._alive_head = None
        self._alive_tail = None
        self._pending_head = None
        self._pending_tail = None
        self._current_pending = None
        self._channels = []
        self._csize = 0
        self._current = -1
        self._size = 0
        self._times = 0
        self._times2 = 0


    class Task(object):
        def __init__(self, thread, target, p):
            self.thread = thread
            self.target = target
            self.p = p
            self.channel = None
            self.next = None
            self.last = None
            
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
        self._live(task)
        self._size += 1
    
    def channel(self):
        p = self._csize
        c = self.Channel(self, p)
        self._channels.append(c)
        self._csize += 1
        return c
        
    def _wakeup(self, task):
        self._live(task)
        self._nopend(task)
        
    def _live(self, task):
        if self._alive_head is None:
            self._alive_head = task
            self._alive_tail = task
            task.next = task
            task.last = task
        else:
            task.next = self._alive_head
            task.last = self._alive_tail
            self._alive_tail.next = task
            self._alive_tail = task
            
    def _nolive(self, task):
        if self._alive_head == self._alive_tail:
            self._alive_head = None
            self._alive_tail = None
            del task.next
            del task.last
            return
        if task == self._alive_head:
            self._alive_head = task.next
        elif task == self._alive_tail:
            self._alive_tail = task.last
        last = task.last
        next = task.next
        last.next = next
        next.last = last
        
    def _nopend(self, task):
        if self._pending_head == self._pending_tail:
            self._pending_head = None
            self._pending_tail = None
            self._current_pending = None
            del task.next
            del task.last
            return
        if task == self._pending_head:
            self._pending_head = task.next
        elif task == self._pending_tail:
            self._pending_tail = task.last
        last = task.last
        next = task.next
        last.next = next
        next.last = last
        if task == self._current_pending:
            self._current_pending = next
        

    def _pend(self, task):
        self._nolive(task)
        if self._pending_head is None:
            self._pending_head = task
            self._pending_tail = task
            task.next = task
            task.last = task
            self._current_pending = task
        else:
            task.next = self._pending_head
            task.last = self._pending_tail
            self._pending_tail.next = task
            self._pending_tail = task
        
    def _done(self, task):
        self._nolive(task)
        
    def _dealmsg(self, task):
        msg = task.channel.msgs.pop(0)
        self._wakeup(msg.source)
        recver = task.channel.recvers.pop(0)
        recver.set(msg.content)
        self._nopend(task)

    def _fetch(self):
        task = self._alive_head
        if task is not None:
            self._nolive(task)
            return task
        
        task = self._current_pending
        while True:
            if task.channel and task.channel.recvers and task.channel.msgs:
                self._dealmsg(task)
                return task
            task = task.next
            if task == self._current_pending:
                break
        return None

    def _recv(self, task, channel, recver):
        if not isinstance(recver, Recv):
            raise self.BadYield('the receiver must be an instance of the class Recv')
        channel.recvers.append(recver)
        task.channel = channel
        self._pend(task)
        self._times2 += 1

    def _send(self, task, channel, msg):
        task.channel = None
        self._pend(task)
        channel.msgs.append(self.Msg(task, msg))
        self._times += 1

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
                        raise self.BadYield()
            except self.BadYield, by:
                raise by
            except StopIteration:
                self._done(task)
            except Exception, ex:
                print type(ex), ex
        if self._alive_head:
            raise self.DeadLock('There is no sender but receiver or no receiver but sender!')
        print 'run times:', self._times, self._times2
        
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
    for i in xrange(1):
        #print 'send', channel, x, 'task2'
        yield send, channel, x
    #print 'task2 end:', x
    yield

def task3(channel):
    for i in xrange(100000):
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
    for i in xrange(20000):
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
