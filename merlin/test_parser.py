import unittest
from zipfile import ZipFile

from merlin.parser import MetricParser


class TestParser(unittest.TestCase):
    def test_something(self):
        MetricParser().load_metrics(metric_db="yaml/metric_definition.yml")

    def test_zip_reading(self):
        sql = "query_test.sql"
        with ZipFile("../test/sql.zip", 'r') as zip_file:
            query = zip_file.read(f"sql/{sql}")
            # TODO: import from moduels folder first then move it to zip

    def test_parser_get_stage(self):
        mp = MetricParser()
        definitions = mp.load_metrics(metric_db="yaml/metric_definition.yml")
        print(definitions)


if __name__ == '__main__':
    unittest.main()
