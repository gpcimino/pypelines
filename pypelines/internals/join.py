import os
import multiprocessing

from .dag import DAGNode, DAGStopIteration


class Join(DAGNode):

    def __init__(self):
        super().__init__()
        self.process = None
        self._async_queue = multiprocessing.Queue()
        self._parallel_jobs = []
        self._stop_iter_received = []
        self._num = multiprocessing.Value('i', 0)
        self._pids = multiprocessing.Array('i', [0] * 32) #ugly max 32 parallel process: use num of cores*x

    def pids(self):
        with self._pids.get_lock():
            res = []
            for i in range(len(self._pids)):
                res.append(self._pids[i])
            return res

    def pid_exists(self, pid):
        with self._pids.get_lock():
            for i in range(len(self._pids)):
                if pid == self._pids[i]:
                    return True
            return False

    def pid_add(self, pid):
        with self._pids.get_lock():
            for i in range(len(self._pids)):
                if self._pids[i] == 0:
                    self._pids[i] = pid
                    return i
            return -1

    def num_father_process(self):
        with self._pids.get_lock():
            res = 0
            for i in range(len(self._pids)):
                if self._pids[i] > 0:
                    res += 1
            return res

    def on_data(self, d):
        pid = os.getpid()
        pids = self.pids()

        create_process = 0
        with self._num.get_lock():
            if self._num.value == 0:
                self._num.value += 1
                create_process = 1

        if create_process == 1:
            create_process = 2
            self.process = multiprocessing.Process(target=self.forward_data, args=(None,))
            self.process.start()

        if not self.pid_exists(pid):
            self.pid_add(pid)
        self._async_queue.put(d)

    def on_completed(self, data=None):
        self._async_queue.put(DAGStopIteration(data))

    def forward_data(self, d):

        done = False
        while not done:
            dd = self._async_queue.get()
            if type(dd) is DAGStopIteration:
                self._stop_iter_received.append(dd)

                if len(self._stop_iter_received) == self.num_father_process():
                    self.forward_completed(self._stop_iter_received)
                    done = True
                else:
                    pass
            else:
                for c in self._childs:
                    c.on_data(dd)

    def forward_completed(self, data=None):
        for c in self._childs:
            c.on_completed(data)

     