from .dag import DAGNode

class MapWithState(DAGNode):
    #lambda s: key selection, lambda s: value selection, lambda key, old, new: new value selection
    def __init__(self, init_value_func, new_value_func):
        super().__init__()
        self._init_value_func = init_value_func
        self._new_value_func = new_value_func
        self._state = {}

    def on_data(self, data):
        key = data[0]
        value = data[1]

        if not key in self._state:
            self._state[key] = self._init_value_func(value)
        else:
            old_value = self._state[key]
            new_value = self._new_value_func(old_value, value)
            self._state[key] = value
            self.forward_data((key, new_value))

    def state(self):
        return self._state
