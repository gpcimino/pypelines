import requests
import urllib.request
from ..internals.dag import DAGNode


class HTTPClient(DAGNode):
    def __init__(self, url):
        super().__init__()
        self._url = url
        self.call_forward = False

    def forward_completed(self, data=None):
        if self.call_forward:
            for c in self._childs:
                c.on_completed(data)

    def produce(self):
        r = requests.get(self._url)
        if r.status_code == 200:
            self.forward_data(r.text)
            self.forward_completed()
        else:
            self.on_error(ex=None)
        # r = requests.get(self._url, stream=True)
        # for line in r.iter_lines():
        #     if line:
        #         print(line)
        #         self.forward_data(line)

        # resp = urllib2.urlopen(self._url)
        # for line in resp: 
        #     self.forward_data(line)

        # with urllib.request.urlopen(self._url) as f:
        #     data = f.read(100).decode('utf-8')
        #     #print(data)
        #     self.forward_data(data)

        
