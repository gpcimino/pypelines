import os
import sys
import time
from datetime import datetime
import threading
import warnings


import traceback
import types
import queue
#from .web import run_web_server
#from .dagnode_async import DAGNodeAsync

class DAGNode():
    caching = 0

    def __init__(self):
        self._father = None
        self._childs = []
        self.cache = None

    def init_cache(self):
        if DAGNode.caching > 0:
            for n in self.root().traverse_preorder():
                n.cache = queue.Queue(maxsize=DAGNode.caching)

    def save_on_cache(self, data):
        if not self.cache is None:
            if self.cache.full():
                self.cache.get()
            self.cache.put((datetime.utcnow(), data,))

    def cache_iter(self):
        if self.cache:
            return self.cache.queue
        else:
            return ['no data received yet']


    def is_leaf(self):
        return self._childs is None or len(self._childs) == 0

    def leafs(self):
        return self._leafs(self)

    def _leafs(self, node):
        if node.is_leaf():
            return [node]
        else:
            ll = []
            for c in node._childs:
                ll.extend(self._leafs(c))
            return ll

    def is_root(self):
        return self._father is None

    def root(self):
        return self._root(self)

    def _root(self, node):
        if node.is_root():
            return node
        else:
            return self._root(node._father)

    def add_child(self, child):
        self._childs.append(child)
        child._father = self

    def __or__(self, other):
        #ensure head of other dag is attached to father node
        other_root = other.root()
        self.add_child(other_root)
        return other

    #deprecated
    def __truediv__(self, other):
        warnings.warn("Division operator is deprecated, use pipe |")
        traceback.print_stack()
        #ensure head of other dag is attached to father node
        other_root = other.root()
        self.add_child(other_root)
        return other

    def __mul__(self, other):
        #create the dag
        self.add_child(other)
        #add to child object (other) a thread safe queue for multi threading comunication
        other.async_queue = queue.Queue()
        #renmae child object (other) method on_data, it will be called by async_receive_impl
        other.old_on_data = other.on_data
        #on data will just put the data obj in the queue
        def async_send(self, data):
            #print(self.async_queue.qsize())
            self.async_queue.put(data)

        other.on_data = types.MethodType(async_send, other)

        #create a new method in child object (other) running in separate thread,
        #which will pop from the queue and call the original on_data method
        def async_receive_impl(self):
            while True:
                dd = self.async_queue.get()
                if type(dd) is StopIteration:
                    break
                self.old_on_data(dd)
                #if self.stop.isSet():
                #    break

        other.async_receive = types.MethodType(async_receive_impl, other)

        #add event for thread synchronization in child object (other)
        other.stop = threading.Event()
        #re-define on_completed in child object (other):
        #- fisrt send stop msg to thread and than
        #- wait for its termination
        #- than call real on_completed
        other.old_on_completed = other.on_completed
        def async_on_completed(self, data=None):
            #self.stop.set()
            self.async_queue.put(StopIteration())
            self.thread.join()
            self.old_on_completed(data)
        other.on_completed = types.MethodType(async_on_completed, other)

        other.thread = threading.Thread(target=other.async_receive)
        print("Start thread " + str(other.thread))
        other.thread.start()

        return other


    def run(self, embed_web_server=False, port=7788, cache_size=0):
        if self.is_root():
            if embed_web_server:
                DAGNode.caching = 1 if cache_size == 0 else cache_size
                self.init_cache()
                #run_web_server(self, port)
            self.produce()
        else:
            self._father.run(embed_web_server, port, cache_size)

    def produce(self):
        pass

    def traverse_preorder(self):
        for n in self._traverse_preorder(self):
            yield n

    def _traverse_preorder(self, node):
        yield node
        for c in node._childs:
            yield from self._traverse_preorder(c)

    def depth(self):
        return self._depth(self)

    def _depth(self, node):
        if node.is_root():
            return 0
        else:
            return self._depth(node._father) + 1

    def forward_data(self, data):
        self.save_on_cache(data)
        for c in self._childs:
            try:
                #print(self.name() + " forward " + str(data) + " to " + c.name())
                c.on_data(data)
            except Exception as ex:
                #if exception is not caught inside node's on_data() method,
                #it is considered a fatal exception
                #so log it as fatal
                print(ex)
                #print("err: " + str(ex))
                #traceback.print_exc()
                #do we  want to propagate ex down stream, only in the branch under c?
                #c.on_error(ex)
                #...and raise to up stream
                raise ex

    def forward_error(self, ex):
        for c in self._childs:
            c.on_error(ex)

    def forward_completed(self, data=None):
        for c in self._childs:
            c.on_completed(data)

    def on_data(self, data):
        pass

    def on_error(self, ex):
        self.forward_error(ex)

    def on_completed(self, data=None):
        #self.log("on completed:" + str(data))
        self.forward_completed(data)

    def dinasty(self):
        return self._dinasty(self)

    def _dinasty(self, node):
        if node.is_root():
            return node.name()
        else:
            return self._dinasty(node._father) + "/" + node.name()

    def find(self, name):
        for node in self.traverse_preorder():
            if node.typename() == name:
                return node

        raise Exception("Cannnot find node " + name)


    def typename(self):
        return type(self).__name__

    def name(self):
        return type(self).__name__

    def father(self):
        return self._father

    def childs(self):
        return self._childs

    def query(self, expr):
        return self._query(self, expr)

    def _query(self, node, expr):
        tokens = expr.split('/')
        if len(tokens) == 0:
            raise Exception("DAG query severe failure")
        elif len(tokens) == 1:
            key = tokens[0]
            if node.name() == key:
                return node
            else:
                return None
        else:
            expr = "/".join(expr.split('/')[1:])
            for c in node._childs:
                res = self._query(c, expr)
                if not res is None:
                    return res

    def log(self, msg):
        print("[{0}] {1} - {2} childs:{3}; {4}".format(time.time(), os.getpid(), type(self).__name__, len(self._childs), msg))

class DAGStopIteration():
    def __init__(self, on_completed_data):
        self.on_completed_data = on_completed_data