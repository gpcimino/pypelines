import unittest

from pypelines import Iterable, AsList, Map, Filter, Assert, Distinct, Sort


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
        consumer = Assert(self, [1, 2, 3, None], ignore_on_completed_data=False)

        producer.add_child(consumer)
        producer.run()

    def test_distinct(self):
        # result = []
        wf = Iterable([1, 2, 3]) | Distinct() | Assert(self, [1, 2, 3])
        wf.run()
        # self.assertEqual([1, 2, 3], result)

    def test_distinct_two_elements_are_same(self):
        result = []
        wf = Iterable([1, 2, 3, 2]) | Distinct() | AsList(result)
        wf.run()
        self.assertEqual([1, 2, 3], result)

    def test_distincts_tuples(self):
        result = []
        wf = Iterable([(1, "abc"), (2, "abc"), (3, "ccc"), (1, "abc"), (1, "qqq")]) | Distinct() | AsList(result)
        wf.run()
        self.assertEqual(sorted([(1, "abc"), (2, "abc"), (3, "ccc"), (1, "qqq")]), sorted(result))

    def test_distinct2(self):
        result = []
        wf = Iterable([("2016-01-01T10:00:05", 100), ("2016-01-01T10:00:05", 101)]) | Distinct() | AsList(result)
        wf.run()
        self.assertEqual(sorted([("2016-01-01T10:00:05", 100), ("2016-01-01T10:00:05", 101)]), sorted(result))

    def test_sort(self):
        result = []
        wf = Iterable([1, 3, 2, 4]) | Sort() | AsList(result)
        wf.run()
        self.assertEqual([1, 2, 3, 4], result)

    def test_sort_non_distinct(self):
        result = []
        wf = Iterable([1, 3, 2, 4, 2, 2]) | Sort() | AsList(result)
        wf.run()
        self.assertEqual([1, 2, 2, 2, 3, 4], result)

if __name__ == "__main__":
    unittest.main()
