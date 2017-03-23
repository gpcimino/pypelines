import logging

from ..internals.dag import DAGNode

class TextFileReaderBase(DAGNode):
    def __init__(self, encoding="utf-8"):
        super().__init__()
        self._encoding = encoding

    def __getstate__(self):
        """ This is called before pickling. """
        #handle how this class is pickled (pickling happen during creation of processes).
        #Removeing file pointer is important to avoid "TypeError: cannot serialize '_io.TextIOWrapper' object" when object is deserialized in the spawned process
        state = self.__dict__.copy()
        if '_fp' in state:
            del state['_fp']
        return state

    def __setstate__(self, state):
        """ This is called while unpickling. """
        self.__dict__.update(state)


class TextFileProducer(TextFileReaderBase):
    def __init__(self, filepath, encoding="utf-8"):
        super().__init__(encoding)
        self._filepath = filepath

    def produce(self):
        #todo: handle exceptions
        #do not declare file pointer in __init__ to pickle object easily during creation of processes
        log = logging.getLogger(__name__)
        self._fp = open(str(self._filepath))
        while True:
            line = self._fp.readline()
            if not line:
                break
            self.forward_data(line)

        self._fp.close()
        self.forward_completed()

    def on_completed(self, data=None):
        log = logging.getLogger(__name__)
        log.debug("ready to close " + str(self._filepath))
        if hasattr(self, '_fp') and not self._fp is None:
            self._fp.close()
            log.debug("closed " + str(self._filepath))
        self.forward_completed(data)


class MultipleTextFileReader(TextFileReaderBase):
    def __init__(self, encoding="utf-8"):
        super().__init__(encoding)

    def on_data(self, data):
        log = logging.getLogger(__name__)
        log.debug("Open file: " + str(data))
        fp = open(str(data))
        lines = ""
        while True:
            line = fp.readline()
            lines += line
            if not line:
                break
        fp.close()
        log.debug("Closed file: " + str(data))
        self.forward_data(lines)

class FlatMultipleTextFileReader(TextFileReaderBase):
    def __init__(self, encoding="utf-8"):
        super().__init__(encoding)

    def on_data(self, data):
        log = logging.getLogger(__name__)
        log.debug("Open file: " + str(data))
        fp = open(str(data))
        while True:
            line = fp.readline()
            if not line:
                break
            self.forward_data(line)

        fp.close()
        log.debug("Closed file: " + str(data))