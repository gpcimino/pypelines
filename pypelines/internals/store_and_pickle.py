import tempfile
import pickle
import os

from .dag import DAGNode

class StoreAndPickle(DAGNode):

    def __init__(self, ignore_on_completed_data=True):
        super().__init__()
        self._ignore_on_completed_data = ignore_on_completed_data
        self._actual = []
        with tempfile.TemporaryFile(prefix='~pypelines_', suffix=".tmp") as tmpf:
            self.pickle_filename = tmpf.name

    def on_data(self, data):
        self._actual.append(data)

    def on_completed(self, data=None):
        if not self._ignore_on_completed_data:
            self._actual.append(data)
        with open(self.pickle_filename, "wb") as f:
            pickle.dump(self._actual, f)

    def load(self):
        with open(self.pickle_filename, "rb") as f:
            actual = pickle.load(f)
        os.remove(self.pickle_filename)
        return actual

