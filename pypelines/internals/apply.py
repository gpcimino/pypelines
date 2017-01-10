from pickleablelambda import pickleable

from .dag import DAGNode


class Apply(DAGNode):
    def __init__(self, on_data=None, on_completed=None, on_error=None):
        super().__init__()
        self._on_data = pickleable(on_data)
        self._on_completed = pickleable(on_completed)
        self._on_error = pickleable(on_error)

    def on_data(self, data):
        if self._on_data:
            self.forward_data(self._on_data(data))


    def on_completed(self, data=None):
        if self._on_completed:
            self.forward_completed(self._on_completed(data))
