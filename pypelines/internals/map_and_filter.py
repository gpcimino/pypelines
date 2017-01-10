from pickleablelambda import pickleable

from .dag import DAGNode

class MapAndFilter(DAGNode):
    def __init__(self, map_func, _filter_func):
        super().__init__()
        self._map = pickleable(map_func)
        self._filter = pickleable(_filter_func)

    def on_data(self, data):
        res = self._map(data)
        if self._filter(res):
            self.forward_data(res)
