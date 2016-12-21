from .dag import DAGNode

class Distinct(DAGNode):
    def __init__(self):
        super().__init__()
        self._distinct_items = set()

    def on_data(self, data):
        self._distinct_items.add(data)

    def on_completed(self, data=None):
        for u in self._distinct_items:
            self.forward_data(u)
        self.forward_completed(data)

