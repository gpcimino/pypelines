import os
import logging
from .dag import DAGNode, DAGStopIteration

class Spawn(DAGNode):

    def __init__(self):
        super().__init__()
        self.spawned = None
        self._async_queue = self.create_queue()
        self.join_on_child_process = True

    def create_queue(self):
        pass

    def spawn(self, target, args=None):
        pass

    def on_data(self, d):
        if not self.spawned:
            #start process if is first call
            self.spawned = self.spawn(target=self.forward_data, args=(self._async_queue,))
            #threading.Thread(target=self.forward_data, args=(None,))
            self.spawned.start()
            #print("Create process " +  str(self.process.pid))

        self._async_queue.put(d)

    def on_completed(self, d=None):
        log = logging.getLogger(__name__)
        log.debug("enter in on_completed in spawn")
        if not self.spawned:
            #here just keep call on completed on childs nodes
            raise Exception(self.name() + " Call on_completed without call on_data first, something strange happened!")
        log.debug("put(DAGStopIteration(d)")
        self._async_queue.put(DAGStopIteration(d))

        if self.join_on_child_process:
            #Join here to wait that all child process are done (i.e. died).
            #In case of symmetric parallelism do not join (i.e. wait) because the collector process 
            #is still alive, and join would block indefinitively
            log.debug("join")
            self.spawned.join()
            log.debug("after join")

    def join(self):
        self.spawned.join()

    def forward_data(self, d):
        log = logging.getLogger(__name__)
        q = d
        log.debug("process/thread created for " + str(self._childs[0].name))
        while True:
            dd = q.get()
            if type(dd) is DAGStopIteration:
                log.debug("received stop iter")
                self.forward_completed(dd.on_completed_data)
                log.debug("exit")
                break
            for c in self._childs:
                c.on_data(dd)
        log.debug("exit")


    def forward_completed(self, data=None):
        for c in self._childs:
            c.on_completed(data)

    def name(self):
        if self._father is None:
            return "None father " + "->" + super().name() + "; pid=" + str(os.getpid())
        else:
            return self._father.name() + "->" + super().name() + "; pid=" + str(os.getpid())
