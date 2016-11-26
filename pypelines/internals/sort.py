from collections import OrderedDict

from .dag import DAGNode

class Sort(DAGNode):
    def __init__(self, key_func=None, reverse=False):
        super().__init__()
        self._store = OrderedDict()
        self._key_func = key_func
        self._reverse = reverse

    def on_data(self, data):
        self._store[self._key_func(data)] = data


    def on_completed(self, data=None):
        keys = reversed(self._store.keys()) if self._reverse else self._store.keys()
        for k in keys:
            self.forward_data(self._store[k])
        self.forward_completed(data)
