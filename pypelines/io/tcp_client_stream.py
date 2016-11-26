from datetime import datetime, timedelta
import time
from socket import * 
import traceback
import logging
from ..internals.dag import DAGNode


class TCPClientStream(DAGNode):
    def __init__(self, address, port, handshake=None, encode_string='ascii', buffer_size=4096):
        super().__init__()
        self._address = address
        self._port = port
        self._handshake = handshake
        self._encode_string = encode_string
        self._sock = None
        self._buffer_size = buffer_size
        self._last_conn_time = None
        self._eol = "\r\n"

    def _open(self):
        log = logging.getLogger(__name__)
        pause_msec = 1
        while True:
            try:
                log.info("Open connection TCP to " +  self._address + " " +  str(self._port))
                self._sock = socket(AF_INET, SOCK_STREAM)
                connection_error = self._sock.connect_ex((self._address, self._port))
                log.info("TCP connection is open")
                if connection_error == 0:
                    log.info("Connection is open")
                    break
                log.warning("Cannot connect due to error number " + str(connection_error))
            except Exception as ex:
                log.warning("Cannot open TCP connection due to: " + str(ex) , exc_info=True)
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

                sock_as_file = self._sock.makefile(mode='r', encoding=self._encode_string, newline=self._eol) 
                #, , buffering=None,, errors=None)
                log.info("Start reading lines from TCP connection")
                for line in sock_as_file:
                    line = line.strip()
                    self.forward_data(line)

                # for line in self.readline():
                #     self.forward_data(line)
            except Exception as ex:
                log.error("TCP connection broken due to: " + str(ex) , exc_info=True)
            finally:
                self._close()
        self.forward_completed()

    # def readline(self):
    #     resp = self._sock.recv(self._buffer_size)
    #     buffer = resp.decode(self._encode_string)
    #     buffering = True
    #     while buffering:
    #         if self._eol in buffer:
    #             (line, buffer) = buffer.split(self._eol, 1)
    #             yield line
    #         else:

    #             resp = self._sock.recv(self._buffer_size)
    #             more = resp.decode(self._encode_string)
    #             if not more:
    #                 buffering = False
    #             else:
    #                 buffer += more
    #     if buffer:
    #         yield buffer

    # def on_data(self, data):
    #     try:
    #         if not self._sock:
    #             self._open()
    #             if self._handshake:
    #                 self._sock.send(str(self._handshake).encode(self._encode_string))
    #             self._sock.send(str(data).encode(self._encode_string))
    #     except Exception as ex:
    #         traceback.print_exc()
    #         print(ex)
    #         try:
    #             self._close()
    #         except:
    #             pass
    #         self._sock = None

    def on_completed(self, data=None):
        self._close()
        self.forward_completed(data)

# if __name__ == "__main__":
#     cli = TCPClientStream("127.0.0.1", 1000)
#     cli.produce()
    