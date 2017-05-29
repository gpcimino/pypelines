import time
import logging

from pickleablelambda import pickleable

from .dag import DAGNode

class Repeat(DAGNode):
    def __init__(self, stop_condition=None, sleep_sec=5):
        super().__init__()
        self._stop_condition = pickleable(stop_condition)
        self._num_iteration = 0 #no worries! int will switch to long once it will exceed int max size
        self._sleep_sec = sleep_sec

    def add_child(self, child):
        if len(self._childs) > 1:
            raise Exception(self.name() + " cannot have more than one child")
        super().add_child(child)
        self.get_producer().call_forward = False

    def get_producer(self):
        return self._childs[0]

    def produce(self):
        log = logging.getLogger(__name__)
        self._num_iteration = 0
        while True:
            log.debug("Enter in produce infinite loop")
            self._childs[0].produce()
            self._num_iteration += 1
            if self._stop_condition and self._stop_condition(self._num_iteration):
                self.get_producer().call_forward = True
                self.forward_completed()
                break
            log.debug("sleep " + str(self._sleep_sec))
            time.sleep(self._sleep_sec)


