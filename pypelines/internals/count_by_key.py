from .dag import DAGNode
#consider renaming into CountByValue as in Spark. From Spark doc:
#Return the count of each unique value in this RDD as a map of (value, count) pairs.
class CountByKey(DAGNode):
    def __init__(self):
        super().__init__()
        self._state = {}

    def on_data(self, data):
        key = data[0]
        if not key in self._state:
            self._state[key] = 0
        self._state[key] += 1

    def on_completed(self, data=None):
        for k, v in self._state.items():
            self.forward_data((k, v))

        self.forward_completed(data)

    def state(self):
        return self._state
