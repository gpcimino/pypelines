import os
from datetime import timedelta, datetime
from ..internals.dag import DAGNode


class TextFileRotate(DAGNode):
    def __init__(self, path, splitting_policy="daily"):
        super().__init__()
        self._path = path
        self._encoding = 'ascii'
        self._filepointers = {}
        self._splitting_policy = splitting_policy
        self.set_splitting_policy_params()
        self._close_file_safety_time = timedelta(seconds=5)

    def on_data(self, data):
        t = data[0]
        line = data[1]
        time_range = self.date2range(t)
        if  time_range not in self._filepointers:
            self._open(time_range)
        self._write(t, line)
        self._try_close(t)

    def _open(self, time_range):
        filepath = os.path.join(self._path, self.filename(time_range))
        #buffering=1 is line buffered
        fp = open(str(filepath), "w", encoding=self._encoding, buffering=1)
        self._filepointers[time_range] = fp

    def _write(self, t, line):
        time_range = self.date2range(t)
        fp = self._filepointers[time_range]
        fp.write(line + "\n")

    def _try_close(self, t):
        for k in list(self._filepointers.keys()):
            dt = self.range2date(k)
            if t - dt > self._close_after_time():
                print("Try to close " + self._filepointers[k].name)
                self._filepointers[k].close()
                print("closed " + self._filepointers[k].name)
                del self._filepointers[k]

    def _force_close(self):
        for k in list(self._filepointers.keys()):
            print("Try to close " + self._filepointers[k].name)
            self._filepointers[k].flush()
            self._filepointers[k].close()
            print("closed " + self._filepointers[k].name)
            del self._filepointers[k]

    def set_splitting_policy_params(self):
        if self._splitting_policy == "daily":
            self.splitting_policy_time_delta = timedelta(days=1)
            self.splitting_policy_time_format = "%Y%m%d"
        elif self._splitting_policy == "minute":
            self.splitting_policy_time_delta = timedelta(seconds=60)
            self.splitting_policy_time_format = "%Y%m%d%H%M"
        else:
            raise Exception("unknonw splitting policy")

    def _close_after_time(self):
        return self.splitting_policy_time_delta + self._close_file_safety_time

    def date2range(self, t):
        return int(t.strftime(self.splitting_policy_time_format))

    def range2date(self, tr):
        return datetime.strptime(str(tr), self.splitting_policy_time_format)

    def filename(self, tr):
        return str(tr) + ".log"








    def on_completed(self, data=None):
        self._force_close()
        self.forward_completed(data)



    # def on_data(self, data):
    #     if not hasattr(self, '_fp'):
    #         self._ctr = 0
    #         self._fp = open(str(self._filepath), "w", encoding=self._encoding)
    #         if self._header:
    #             self._fp.write(self._header + "\n")
    #     self._ctr += 1
    #     self._fp.write(str(data) + "\n")
    #     if self._ctr % 10 == 0:
    #         self.log(str(self._filepath) + "==>" + str(self._ctr))

    # def on_completed(self, data=None):
    #     self.log("ready to close " + str(self._filepath))
    #     if hasattr(self, '_fp') and not self._fp is None:
    #         self._fp.close()
    #         self.log("closed " + str(self._filepath))
    #     self.forward_completed(data)

