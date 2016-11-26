from .dag import DAGNode

class AsList(DAGNode):
    def __init__(self):
        super().__init__()
        self.list = []

    def on_data(self, data):
        self.list.append(data)
