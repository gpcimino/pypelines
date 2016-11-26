from .dag import DAGNode
import os


class StdOut(DAGNode):
    def __init__(self, prefix=""):
        super().__init__()
        self._prefix = prefix

    def on_data(self, data):
        if not data is None:
            print(self._prefix + str(data))

    def on_completed(self, data=None):
        if not data is None:
            print(self._prefix + str(data))

