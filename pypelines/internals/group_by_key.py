from .dag import DAGNode

class GroupByKey(DAGNode):
    def __init__(self):
        super().__init__()
        self._state = {}

    def on_data(self, data):
        key = data[0]
        value = data[1]
        if not key in self._state:
            self._state[key] = []
        self._state[key].append(value)

    def on_completed(self, data=None):
        for k, v in self._state.items():
            self.forward_data((k, v))
        self.forward_completed(data)

    def state(self):
        return self._state
