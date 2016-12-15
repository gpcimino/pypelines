from .dag import DAGNode

class AsList(DAGNode):
    def __init__(self, store=None):
        super().__init__()
        self.list = []
        self._store = store

    def on_data(self, data):
        self.list.append(data)
        if not self._store is None:
            self._store.append(data)
