import unittest
import io
from pypelines.config import config2dict


class TestConfig(unittest.TestCase):

    def test_config_empty(self):
        empty_config = io.StringIO("")
        d = config2dict(empty_config, )
        self.assertEqual(d, {})

    def test_file_not_exists(self):
        d = config2dict("/all/your/base/are/belong/to/us.conf", )
        self.assertEqual(d, {})

    def test_one_item_flat(self):
        config = io.StringIO('''
        [s1]
        a=b
        ''')
        d = config2dict(config)
        self.assertEqual(d, {'s1__a' : 'b'})

    def test_one_item_merge(self):
        config = io.StringIO('''
        [s1]
        a=b
        ''')
        d = config2dict(config, flat=False)
        self.assertEqual(d, {'a' : 'b'})

    def test_merge_raise_ex(self):
        config = io.StringIO('''
        [s1]
        a=b
        [s2]
        a=2
        ''')
        with self.assertRaises(Exception):
            d = config2dict(config, flat=False)

    def test_default_section(self):
        config = io.StringIO('''
        [DEFAULT]
        a=b
        [S1]
        c=1
        ''')
        d = config2dict(config)
        self.assertEqual(d, {'S1__a' : 'b', 'S1__c' : '1'})


    def test_interpolate(self):
        config = io.StringIO('''
        [S1]
        a=b
        c=%(a)s
        ''')
        d = config2dict(config, interpolate=True)
        self.assertEqual(d, {'S1__a' : 'b', 'S1__c' : 'b'})        

if __name__ == "__main__":
    unittest.main()