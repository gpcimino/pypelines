from .dag import DAGNode

class Filter(DAGNode):
    def __init__(self, func=None):
        super().__init__()
        self._func = func

    def on_data(self, data):
        if self._func(data):
            return self.forward_data(data)
