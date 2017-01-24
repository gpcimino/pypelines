import unittest
import socket
import threading
import os


from pypelines import AsList
from pypelines.io import TCPClientStream


class TestTCPClinetStream(unittest.TestCase):

    def setUp(self):
        self._data = None
        self._records = None
        self._buffer_size = 7
        TCPClientStream.read = self.fake_read

    def test_tcp_client_readline(self):
        self._data = "hello\nworld"
        client = TCPClientStream("127.0.0.1", 10000, eol="\n")
        res = [x for x in client.readline()]
        self.assertEqual(res, ["hello", "world"])

    def test_4parts(self):
        self._data = "the quick brown\n fox jump over\n the lazy \ndog"
        client = TCPClientStream("127.0.0.1", 10000, eol="\n")
        res = [x for x in client.readline()]
        self.assertEqual(res, ["the quick brown", " fox jump over", " the lazy ", "dog"])

    def test_data_start_with_garbage(self):
        self._data = "\u0394\nthe quick\nbrown fox"
        client = TCPClientStream("127.0.0.1", 10000, eol="\n")
        res = [x for x in client.readline()]
        self.assertEqual(res, ["\u0394", "the quick", "brown fox"])

    def test_end_with_eol(self):
        self._data = "the quick brown fox \n jump over the lazy dog\n"
        client = TCPClientStream("127.0.0.1", 10000, eol="\n")
        res = [x for x in client.readline()]
        self.assertEqual(res, ["the quick brown fox ", " jump over the lazy dog"])

    def test_start_with_eol(self):
        self._data = "\nthe quick brown fox \n jump over the lazy dog"
        client = TCPClientStream("127.0.0.1", 10000, eol="\n")
        res = [x for x in client.readline()]
        self.assertEqual(res, ["", "the quick brown fox ", " jump over the lazy dog"])

    def test_custom_eol(self):
        self._data = "the quick brown fox $! jump over the lazy dog"
        client = TCPClientStream("127.0.0.1", 10000, eol="$!")
        res = [x for x in client.readline()]
        self.assertEqual(res, ["the quick brown fox ", " jump over the lazy dog"])

    def test_start_with_half_eol(self):
        self._data = "!the quick brown fox $! jump over the lazy dog"
        client = TCPClientStream("127.0.0.1", 10000, eol="$!")
        res = [x for x in client.readline()]
        self.assertEqual(res, ["!the quick brown fox ", " jump over the lazy dog"])

    def test_double_eol(self):
        self._data = "the quick brown fox\n\njump over the lazy dog"
        client = TCPClientStream("127.0.0.1", 10000, eol="\n")
        res = [x for x in client.readline()]
        self.assertEqual(res, ["the quick brown fox", "", "jump over the lazy dog"])


    def fake_readOLD(self):
        if not self._records:
            self._records = [self._data[i:i+self._buffer_size] for i in range(0, len(self._data), self._buffer_size)]
        for r in self._records:
            yield r
        yield None

    def fake_read(self):
        if self._records is None:
            self._records = [self._data[i:i+self._buffer_size] for i in range(0, len(self._data), self._buffer_size)]
        if len(self._records) == 0:
            return None
        res = self._records.pop(0)
        return res        

if __name__ == "__main__":
    unittest.main()
