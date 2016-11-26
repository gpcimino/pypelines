import unittest

from pypelines.internals.spawn_process import SpawnProcess
from pypelines.internals.iterable import Iterable
from pypelines.internals.map import Map
from pypelines.internals.store_and_pickle import StoreAndPickle


def add100(n):
    return n + 100

class TestAsymmetricParallelismWithProcesses(unittest.TestCase):

    def test_producer_consume(self):
        data = [1, 2, 3, 4, 5]

        workflow = Iterable(data) | SpawnProcess() | StoreAndPickle()
        workflow.run()

        #workflow ref to StoreAndPickle() instace that is the only leaf of the DAG
        actual = workflow.load()
        self.assertEqual(actual, data)

    def test_producer_map_consume_with_2_process(self):
        data = [1, 2, 3, 4, 5]

        #CAUTION!!!!
        #Cannot use lambda (e.g. Map(lambda x: x+100)) yet due to pickle problem in multiprocessing lib
        #possible solution is to hook the import of pickle im multiprocessing lib
        #and substitute with dill.
        #See: #http://chimera.labs.oreilly.com/books/1230000000393/ch10.html#_solution_180
        workflow = Iterable(data) | SpawnProcess() | Map(add100) | StoreAndPickle()
        workflow.run()

        #workflow ref to StoreAndPickle() instace that is the only leaf of the DAG
        actual = workflow.load()
        self.assertEqual(actual, [d+100 for d in data])

    def test_producer_map_consume_with_3_process(self):
        data = [1, 2, 3, 4, 5]

        workflow = Iterable(data) | SpawnProcess() | Map(add100) | SpawnProcess() | StoreAndPickle()
        workflow.run()

        #workflow ref to StoreAndPickle() instace that is the only leaf of the DAG
        actual = workflow.load()
        self.assertEqual(actual, [d+100 for d in data])

    def test_producer_map_consume_with_3_process_plus_1_brach(self):
        data = [1, 2, 3, 4, 5]

        workflow = Iterable(data)
        sync_brach = workflow | StoreAndPickle()
        async_branch = workflow | SpawnProcess() | Map(add100) | SpawnProcess() | StoreAndPickle()
        workflow.run()

        self.assertEqual(sync_brach.load(), data)
        self.assertEqual(async_branch.load(), [d+100 for d in data])

    def test_producer_map_consume_with_3_process_plus_2_braches(self):
        data = [1, 2, 3, 4, 5]

        workflow = Iterable(data)
        sync_brach = workflow | StoreAndPickle()
        async_branch = workflow | SpawnProcess() | Map(add100)
        async_branch1 = async_branch | StoreAndPickle()
        async_branch2 = async_branch | Map(add100) | SpawnProcess() | StoreAndPickle()
        workflow.run()

        self.assertEqual(sync_brach.load(), data)
        self.assertEqual(async_branch1.load(), [d+100 for d in data])
        self.assertEqual(async_branch2.load(), [d+200 for d in data])

if __name__ == "__main__":
    unittest.main()




# class TestAsymmetricParallelismWithProcesses():
#     def test_producer_consumer(self):
#         data = [1, 2, 3, 4, 5]
#         workflow = Iterable(data) + StoreAndAssert(data, self)
#         workflow.run()

#     def assertEqual(self, actual, expected):
#         if isinstance(actual, collections.Iterable) and isinstance(expected, collections.Iterable):
#             for a,e in zip(actual, expected):
#                 if a != e:
#                     raise Exception("failure")
#         else:
#             if actual != expected:
#                     raise Exception("failure")
#         print("ok")


# if __name__ == "__main__":
#     unit_test = TestAsymmetricParallelismWithProcesses()
#     unit_test.test_producer_consumer()