from .dag import DAGNode

class Assert(DAGNode):
    def __init__(self, value):
        super().__init__()
        self._value = value
        self.list = []

    def on_data(self, data):
        self.list.append(data)

    def on_completed(self, data=None):
        if self.list != self._value:
            raise Exception("DAG assert failed:" + str(self.list) + " != " + str(self._value))
        else:
            #print("OK: " + str(self.list) + " != " + str(self._value))
            pass
