from .dag import DAGNode

class Sort(DAGNode):
    def __init__(self, key_func=None, reverse=False):
        super().__init__()
        self._store = []
        self._key_func = key_func
        self._reverse = reverse

    def on_data(self, data):
        #self._store[self._key_func(data)] = data
        self._store.append(data)


    def on_completed(self, data=None):
        for item in sorted(self._store, key=self._key_func, reverse=self._reverse):
            self.forward_data(item)
        self.forward_completed(data)
        # for key in sorted(self._store, reverse=self._reverse):
        #     print(self._store[key])
        #     self.forward_data(self._store[key])
        #     print("after sort")
        # self.forward_completed(data)
