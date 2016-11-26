import unittest

from pypelines.internals.iterable import Iterable
from pypelines.internals.aslist import AsList
from pypelines.internals.map import Map
from pypelines.internals.filter import Filter
from pypelines.internals.store_and_assert import StoreAndAssert


class TestDAG(unittest.TestCase):

    def test_iter2list(self):
        producer = Iterable([1, 2, 3])
        tolist = AsList()
        producer.add_child(tolist)
        producer.run()
        self.assertEqual(tolist.list, [1, 2, 3])

    def test_iter_filter_list(self):
        producer = Iterable([1, 2, 3])
        flt = Filter(lambda x: x > 1)
        tolist = AsList()

        producer.add_child(flt)
        flt.add_child(tolist)

        producer.run()
        self.assertEqual(tolist.list, [2, 3])


    def test_iter_filter_map_list(self):
        producer = Iterable([1, 2, 3])
        flt = Filter(lambda x: x > 1)
        map = Map(lambda x: x+10)
        tolist = AsList()

        producer.add_child(flt)
        flt.add_child(map)
        map.add_child(tolist)

        producer.run()

        self.assertEqual(tolist.list, [12, 13])

    def test_error(self):
        producer = Iterable(['a', 'b', 'c'])
        flt = Filter(lambda x: int(x) > 1)
        tolist = AsList()
        producer.add_child(flt)
        flt.add_child(tolist)
        with self.assertRaises(ValueError):
            producer.run()

    def test_two_lists(self):
        producer = Iterable([1, 2, 3])
        flt = Filter(lambda x: x > 1)
        l1 = AsList()
        l2 = AsList()

        producer.add_child(l1)
        producer.add_child(flt)
        flt.add_child(l2)

        producer.run()

        self.assertEqual(l1.list, [1, 2, 3])
        self.assertEqual(l2.list, [2, 3])

    def test_on_completed(self):
        producer = Iterable([1, 2, 3])
        consumer = StoreAndAssert([1, 2, 3, None], self, "assertEqual", ignore_on_completed_data=False)

        producer.add_child(consumer)
        producer.run()

if __name__ == "__main__":
    unittest.main()
