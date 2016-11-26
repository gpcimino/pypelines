import queue
import threading

from .spawn import Spawn

class SpawnThread(Spawn):

    def __init__(self):
        super().__init__()

    def create_queue(self):
        return queue.Queue()

    def spawn(self, target, args=None):
        return threading.Thread(target=target, args=args)
