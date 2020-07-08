import unittest

from merlin.metric import Definition, Stage
from merlin.parser import MetricParser


class MetricParserTest(unittest.TestCase):
    def test_loading(self):
        parser = MetricParser("../metrics.yaml", "../metric_sql.zip", None)
        metric_definition = parser.load_metrics()
        self.assertIsInstance(metric_definition, list)
        for e in metric_definition:
            self.assertIsInstance(e, Definition)
            self.assertIsNotNone(e.metric.metric_id)
            self.assertIsNotNone(e.stages)
            for s in e.stages:
                self.assertIsInstance(s, Stage)
                print(s.sql_query)



if __name__ == '__main__':
    unittest.main()
