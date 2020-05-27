import unittest
from zipfile import ZipFile
from importlib import import_module

from merlin.parser import MetricParser


# TODO: make sure that this path aligns with the
EXTRA_LIB_PATH = "/modules.zip"
if os.path.exists(EXTRA_LIB_PATH):
    sys.path.append(EXTRA_LIB_PATH)

import DefaultStage

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
        mp.load_metrics(metric_db="yaml/metric_definition.yml")


if __name__ == '__main__':
    unittest.main()
