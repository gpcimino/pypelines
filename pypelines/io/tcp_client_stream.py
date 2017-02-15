from datetime import datetime, timedelta
import os
import time
from socket import * 
import traceback
import logging
from ..internals.dag import DAGNode


class TCPClientStream(DAGNode):
    def __init__(self, address, port, handshake=None, encode_string='ascii', buffer_size=1024, eol=os.linesep, timeout_sec=30):
        super().__init__()
        self._address = address
        self._port = port
        self._handshake = handshake
        self._encode_string = encode_string
        self._sock = None
        self._buffer_size = buffer_size
        self._last_conn_time = None
        self._eol = eol
        self._timeout_sec = timeout_sec

    def address_str(self):
        return str(self._address) + ":" +  str(self._port)

    def _open(self):
        log = logging.getLogger(__name__)
        pause_msec = 1
        while True:
            try:
                log.info("Opening connection TCP to " +  self.address_str())
                self._sock = socket(AF_INET, SOCK_STREAM)
                self._sock.settimeout(self._timeout_sec)
                try:
                    self._sock.connect((self._address, self._port))
                    log.info("Connection to " +  self.address_str() + " is open")
                    break
                except Exception as ex2:
                    log.warning("Cannot connect due to error: " + str(ex2), exc_info=True)
            except Exception as ex:
                log.warning("Cannot open TCP connection due to: " + str(ex), exc_info=True)
            log.info("Sleep " + str(pause_msec/1000) + " sec")
            time.sleep(pause_msec/1000)
            pause_msec *= 2
            pause_msec = min(60000, pause_msec)

    def _close(self):
        log = logging.getLogger(__name__)
        log.info("Close TCP connection")
        self._sock.shutdown(SHUT_WR)
        self._sock.close()
        log.info("TCP Connection is closed")

    def produce(self):
        log = logging.getLogger(__name__)
        while True:
            self._open()
            try:
                # if not self._handshake is None:
                #     self._sock.send(str(self._handshake).encode(self._encode_string))
                for line in self.readline():
                    self.forward_data(line)
            except Exception as ex:
                log.error("TCP connection broken due to: " + str(ex), exc_info=True)
            finally:
                self._close()
        self.forward_completed()

    def read(self):
        resp = self._sock.recv(self._buffer_size)
        return resp.decode(self._encode_string)

    def readline(self):
        buffer = self.read()
        buffering = True
        while buffering:
            if self._eol in buffer:
                (line, buffer) = buffer.split(self._eol, 1)
                yield line
            else:
                more = self.read()
                if not more:
                    buffering = False
                else:
                    buffer += more
        if buffer:
            yield buffer

    def on_completed(self, data=None):
        self._close()
        self.forward_completed(data)
