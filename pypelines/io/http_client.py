import requests
import urllib.request
from ..internals.dag import DAGNode


class HTTPClient(DAGNode):
    def __init__(self, url, readlines=False):
        super().__init__()
        self._url = url
        self.call_forward = True
        self.produce = self.produce_line_by_line if readlines else self.produce_whole_file

    def forward_completed(self, data=None):
        if self.call_forward:
            for c in self._childs:
                c.on_completed(data)

    def produce_whole_file(self):
        req = requests.get(self._url)
        if req.status_code == 200:
            self.forward_data(req.text)
            self.forward_completed()
        else:
            self.on_error(ex=None)

    def produce_line_by_line(self):
        req = requests.get(self._url, stream=True)
        if req.status_code == 200:
            for line in req.iter_lines():
                if line:
                    self.forward_data(str(line))
            self.forward_completed()
        else:
            self.on_error(ex=None)


