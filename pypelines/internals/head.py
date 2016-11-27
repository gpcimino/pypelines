from .dag import DAGNode

class Head(DAGNode):
    def __init__(self, n=10):
        super().__init__()
        self._counter = 0
        self._max_items = n

    def on_data(self, data):
        if self._counter < self._max_items:
            self._counter += 1
            self.forward_data(data)
