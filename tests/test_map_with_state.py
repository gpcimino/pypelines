import unittest

from pypelines.internals.map_with_state import MapWithState 

# class TestMapWithState(unittest.TestCase):

#     def test_compute_delta_among_records(self):
#         mws = MapWithState(lambda value: value, lambda old, lambda new: new-old)
#         mws.on_data((1, 100))
#         mws.on_data((1, 101))
#         self.assertEqual( mws.state(), {1: 1})


#     def test_compute_delta_among_records_multi_keys(self):
#         mws = MapWithState(lambda value: value, lambda old, new: new-old)
#         mws.on_data((1, 100))
#         mws.on_data((2, 100))
#         mws.on_data((1, 101))
#         mws.on_data((2, 105))
#         self.assertEqual( mws.state(), {1: 1, 2: 5})

if __name__ == "__main__":
    unittest.main()
