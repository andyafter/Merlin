import unittest
from datetime import datetime

from merlin.metric import OutputMetric, SourceMetric
from merlin.values import ComposedValue


class OutputMetricTest(unittest.TestCase):

    def test_asdict(self):
        source_metric = SourceMetric(metric_id="volumetric_flow_rate",
                                     time_window=500,
                                     func_expr="mass / density / time",
                                     horizontal_level=1,
                                     vertical_level=0,
                                     version="1.0",
                                     func_vars=["mass", "density", "time"])

        mass = ComposedValue(25.0, "mass")
        density = ComposedValue(5.0, "density")
        location = ComposedValue([120.0, 10.0], "location")
        output_metric = OutputMetric(source_metric, 5.0, datetime(2020, 3, 25, 1, 0))
        output_metric.add_func_var(mass)
        output_metric.add_func_var(density)
        output_metric.add_group_value(location)
        self.assertIsNotNone(output_metric.asdict())


if __name__ == '__main__':
    unittest.main()
