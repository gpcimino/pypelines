from time import sleep
from .dag import DAGNode

class Sleep(DAGNode):
    def __init__(self, sleep_time_sec=1):
        super().__init__()
        self._sleep_time_sec = sleep_time_sec

    def on_data(self, data):
        sleep(self._sleep_time_sec)
        return self.forward_data(data)

