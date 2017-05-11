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

    def client_address(self):
        return str(self._address) + ":" + str(self._port)

    def _open(self):
        log = logging.getLogger(__name__)
        log.debug("Trying to open connection to " + self.client_address())
        self._sock = socket(AF_INET, SOCK_STREAM)
        self._sock.connect((self._address, self._port))
        log.debug("Connection opened")

    def _close(self):
        try:
            log = logging.getLogger(__name__)
            if self._sock is not None:
                self._sock.close()
                log.debug("Socket closed")
            else:
                log.debug("Socket already closed")
        except:
            log.error("Cannot gracefully close socket. Socket is set to null.")
            self._sock = None

    def on_data(self, data):
        log = logging.getLogger(__name__)
        log.debug("Ready to send data to " + str(self._address) + ":" + str(self._port))
        try:
            self._open()
            if self._handshake:
                self._sock.send(str(self._handshake).encode(self._encode_string))
            sent = self._sock.send(str(data).encode(self._encode_string))
            if sent == 0:
                log.warning("Broken socket connection to "+ self.client_address())
            else:
                log.debug(str(len(data)) + " bytes sent to " + self.client_address())
        except error as sockex: #socket.error
            log.exception(sockex, exc_info=True)
        finally:
            self._close()

        self._sock = None

    def on_completed(self, data=None):
        log = logging.getLogger(__name__)
        log.debug("Ready to send data call on_completed")
        self._close()
        self.forward_completed(data)

