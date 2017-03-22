from pickleablelambda import pickleable

from .dag import DAGNode


class Do(DAGNode):
    def __init__(self, func):
        super().__init__()
        self._func = pickleable(func)

    def produce(self):
        result = self._func()
        self.forward_data(result)
        self.forward_completed()