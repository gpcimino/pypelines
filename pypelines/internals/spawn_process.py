import multiprocessing
#import multiprocessing.managers
#use pathos instead of python multiprocessing std lib
#import multiprocess
#import queue
from .spawn import Spawn

class SpawnProcess(Spawn):

    def __init__(self):
        super().__init__()
        #multiprocessing.current_process().authkey = 'xxxxx'

    def create_queue(self):
        return multiprocessing.Queue()
        #return queue.Queue()
        #return multiprocess.Queue()

        # manager = multiprocess.Manager()
        # return manager.Queue()

        #m = multiprocessing.managers.BaseManager(authkey = 'xxxxx')
        #return m.Queue()


    def spawn(self, target, args=None):
        return multiprocessing.Process(target=target, args=args)

