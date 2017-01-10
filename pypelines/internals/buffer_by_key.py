from pickleablelambda import pickleable

from .dag import DAGNode

class BufferByKey(DAGNode):
    def __init__(self, release_buffer_func):
        super().__init__()
        self._state = {}
        self._release_buffer = pickleable(release_buffer_func)

    def on_data(self, data):
        key = data[0]
        value = data[1]
        if not key in self._state:
            self._state[key] = []
            self._state[key].append(value)
        else:
            if self._release_buffer(self._state[key], value):
                self.forward_data((key, self._state[key]))
                self._state[key] = []
            self._state[key].append(value)

    def on_completed(self, data=None):
        #flush the pending items
        for key in self._state:
            if self._state[key] != []:
                self.forward_data((key, self._state[key]))
        self.forward_completed(data)

