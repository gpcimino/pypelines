from socket import *
import logging

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
        log = logging.getLogger(__name__)
        log.debug("Trying to open connection to " + str(self._address) + ":" + str(self._port))
        self._sock = socket(AF_INET, SOCK_STREAM)
        self._sock.connect((self._address, self._port))
        log.debug("Connection opened")

    def _close(self):
        log = logging.getLogger(__name__)
        if self._sock is not None:
            self._sock.close()
            log.debug("Socket closed")
        else:
            log.debug("Socket already closed")


    # def produce(self):
    #     while True:
    #         try:
    #             self._open()
    #             if self._handshake:
    #                 self._sock.send(str(self._handshake).encode(self._encode_string))
    #                 #print("handshake sent " + str(self._handshake))
    #             while True:
    #                 resp = self._sock.recv(self._buffer_size)
    #                 data = resp.decode(self._encode_string)
    #                 #print("received " + str(data))
    #                 if not data:
    #                     print("connection closed, empty data received")
    #                     break
    #                 self.forward_data(data)
    #             self._close()
    #         except Exception as ex:
    #             traceback.print_exc()
    #             print(ex)
    #             pass
    #     self.forward_completed()

    def on_data(self, data):
        log = logging.getLogger(__name__)
        log.debug("Ready to send data to " + str(self._address) + ":" + str(self._port))
        try:
            self._open()
            if self._handshake:
                self._sock.send(str(self._handshake).encode(self._encode_string))
            self._sock.send(str(data).encode(self._encode_string))
            log.debug("Data sent")
        except Exception as ex:
            log.exception(ex, exc_info=True)
        finally:
            try:
                self._close()
            except:
                pass

        self._sock = None

    def on_completed(self, data=None):
        log = logging.getLogger(__name__)
        log.debug("Ready to send data call on_completed")
        self._close()
        self.forward_completed(data)
    