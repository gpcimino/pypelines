from socket import * 
import traceback
from ..internals.dag import DAGNode


class TCPClient(DAGNode):
    def __init__(self, address, port, handshake=None, encode_string='ascii', buffer_size=1024):
        super().__init__()
        self._address = address
        self._port = port
        self._handshake = handshake
        self._encode_string = encode_string
        self._sock = None
        self._buffer_size = buffer_size

    def _open(self):
        #print("prior to open")
        self._sock = socket(AF_INET, SOCK_STREAM)
        self._sock.connect((self._address, self._port))
        #print("after to open")

    def _close(self):
        #print("prior to close")
        self._sock.close()
        #print("prior to close")

    def produce(self):
        while True:
            try:
                self._open()
                if self._handshake:
                    self._sock.send(str(self._handshake).encode(self._encode_string))
                    #print("handshake sent " + str(self._handshake))
                while True:
                    resp = self._sock.recv(self._buffer_size)
                    data = resp.decode(self._encode_string)
                    #print("received " + str(data))
                    if not data:
                        print("connection closed, empty data received")
                        break
                    self.forward_data(data)
                self._close()
            except Exception as ex:
                traceback.print_exc()
                print(ex)
                pass
        self.forward_completed()

    def on_data(self, data):
        try:
            if not self._sock:
                self._open()
                if self._handshake:
                    self._sock.send(str(self._handshake).encode(self._encode_string))
                self._sock.send(str(data).encode(self._encode_string))
        except Exception as ex:
            traceback.print_exc()
            print(ex)
            try:
                self._close()
            except:
                pass
            self._sock = None

    def on_completed(self, data=None):
        self._close()
        self.forward_completed(data)
    