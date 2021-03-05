import unittest
from sample import *


class TestSample(unittest.TestCase):
    """test class of sample.py"""

    def test_get_cluster(self):
        """Test of get_cluster"""
        expected = "test"

        query = """cluster('test.kusto.windows.net').database('db').Table | limit 10"""
        self.assertEqual(expected, get_cluster(query))

        query = """cluster("test.kusto.windows.net").database('db').Table | limit 10"""
        self.assertEqual(expected, get_cluster(query))

        query = """cluster(test.kusto.windows.net).database('db').Table | limit 10"""
        self.assertEqual(expected, get_cluster(query))

        query = """cluster('test').database('db').Table | limit 10"""
        self.assertEqual(expected, get_cluster(query))

        query = """cluster(test).database('db').Table | limit 10"""
        self.assertEqual(expected, get_cluster(query))

        expected = ""
        query = "Talbe | limit 10"
        self.assertEqual(expected, get_cluster(query))

    def test_get_database(self):
        """Test of get_database"""
        expected = "test"
        query = (
            """cluster('cluster.kusto.windows.net').database('test').Table | limit 10"""
        )
        self.assertEqual(expected, get_database(query))
        query = (
            """cluster('cluster.kusto.windows.net').database("test").Table | limit 10"""
        )
        self.assertEqual(expected, get_database(query))
        query = (
            """cluster('cluster.kusto.windows.net').database(test).Table | limit 10"""
        )
        self.assertEqual(expected, get_database(query))

        expected = ""
        query = "Talbe | limit 10"
        self.assertEqual(expected, get_database(query))


if __name__ == "__main__":
    unittest.main()