from .dag import DAGNode


class Iterable(DAGNode):
    def __init__(self, iterable):
        super().__init__()
        self._iterable = iterable

    def produce(self):
        for data in self._iterable:
            self.forward_data(data)
            #time.sleep(1)
        self.forward_completed()
