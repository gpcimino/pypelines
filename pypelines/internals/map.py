from pickleablelambda import pickleable

from .dag import DAGNode

class Map(DAGNode):
    def __init__(self, func):
        super().__init__()
        self._func = pickleable(func)

    def on_data(self, data):
        res = self._func(data)
        self.forward_data(res)

    def name(self):
        return super().name() #+ "; func=" + inspect.getsource(self._func)

