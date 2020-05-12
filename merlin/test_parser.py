import unittest

from merlin.parser import MetricParser


class TestParser(unittest.TestCase):
    def test_something(self):
        MetricParser().load_metrics(metric_db="yaml/metric_definition.yml")


if __name__ == '__main__':
    unittest.main()
