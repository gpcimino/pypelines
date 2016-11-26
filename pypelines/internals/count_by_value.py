from .dag import DAGNode

#From Spark doc:
#Return the count of each unique value in this RDD as a map of (value, count) pairs.
class CountByValue(DAGNode):
    def __init__(self):
        super().__init__()
        self._state = {}

    def on_data(self, data):
        if not data in self._state:
            self._state[data] = 0
        self._state[data] += 1

    def on_completed(self, data=None):
        for k, v in self._state.items():
            self.forward_data((k, v))

        self.forward_completed(data)

    def state(self):
        return self._state
