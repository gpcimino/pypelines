import unittest

from pypelines.internals.iterable import Iterable
from pypelines.internals.map import Map
from pypelines.internals.parallelize import Parallelize
from pypelines.internals.join import Join
from pypelines.internals.store_and_pickle import StoreAndPickle

def add100(n):
    return n + 100

def no_split(n):
    return 1

def two_split(n):
    return n % 2

def ten_split(n):
    return n % 10


class TestSymmetricParallelismWithProcesses(unittest.TestCase):

    def test_producer_consume_no_parallelism(self):
        data = [1, 2, 3, 4, 5]
        parallel = Map(add100)
        workflow = Iterable(data) | Parallelize(no_split) | parallel | Join() | StoreAndPickle()
        workflow.run()

        #workflow ref to StoreAndPickle() instace that is the only leaf of the DAG
        actual = workflow.load()
        #need to sort result because with symmetric parallelims order is not guaranteed
        self.assertEqual(sorted(actual), [d+100 for d in data])

    def test_producer_consume_2_processes(self):
        data = [1, 2, 3, 4, 5]
        parallel = Map(add100) | Map(add100)
        workflow = Iterable(data) | Parallelize(two_split) | parallel | Join() | StoreAndPickle()
        workflow.run()

        #workflow ref to StoreAndPickle() instace that is the only leaf of the DAG
        actual = workflow.load()
        #need to sort result because with symmetric parallelims order is not guaranteed
        self.assertEqual(sorted(actual), [d+200 for d in data])

    def test_producer_consume_2_processes_inline(self):
        data = [1, 2, 3, 4, 5]
        workflow = Iterable(data) | Parallelize(two_split) | ( Map(add100) | Map(add100) ) | Join() | StoreAndPickle()
        workflow.run()

        #workflow ref to StoreAndPickle() instace that is the only leaf of the DAG
        actual = workflow.load()
        #need to sort result because with symmetric parallelims order is not guaranteed
        self.assertEqual(sorted(actual), [d+200 for d in data])        

    def test_producer_consume_10_processes(self):
        data = range(20)
        parallel = Map(add100)
        workflow = Iterable(data) | Parallelize(ten_split) | parallel | Join() | StoreAndPickle()
        workflow.run()

        #workflow ref to StoreAndPickle() instace that is the only leaf of the DAG
        actual = workflow.load()
        #need to sort result because with symmetric parallelims order is not guaranteed
        self.assertEqual(sorted(actual), [d+100 for d in data])


if __name__ == "__main__":
    import multiprocessing as mp
    mp.freeze_support()
    unittest.main()


