from pickleablelambda import pickleable

from .dag import DAGNode

class Map(DAGNode):
    def __init__(self, func, propagate_none=True):
        super().__init__()
        self._func = pickleable(func)
        self._propagate_none = propagate_none

    def on_data(self, data):
        res = self._func(data)
        if res is None and not self._propagate_none:
            pass
        else:
            self.forward_data(res)

    def name(self):
        return super().name() #+ "; func=" + inspect.getsource(self._func)

