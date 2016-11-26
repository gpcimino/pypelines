from .dag import DAGNode

class Sum(DAGNode):
    def __init__(self, init_value=0):
        super().__init__()
        self._sum = init_value

    def on_data(self, data):
        self._sum += data

    def on_completed(self, data=None):
        self.forward_data(self._sum)




