import os
import sys
import tempfile
import unittest
from datetime import datetime
from zipfile import ZipFile

from pyspark import SparkConf
from pyspark.sql import SparkSession

import merlin.engines.context as ctx
from merlin.engines.spark_stand_alone import SparkStandAlone
from merlin.metric import SourceMetric, Definition
from merlin.stage import Stage, StageType, StageOutputType


class SparkStandAloneTestCase(unittest.TestCase):

    def test_engine(self):
        temp_dir = tempfile.mkdtemp()
        print("Temp dir {}".format(temp_dir))
        conf = SparkConf().setAppName("unit_test").setMaster("local[2]")
        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        test_df = spark.read.json("test/data/synthetic_data.json.gz")
        test_df.createOrReplaceTempView("input_data")
        spark.sql("select from_unixtime(time) time, location_id, data  from input_data").show(10, 80)
        source_metric = SourceMetric(metric_id="p90_sensor_0",
                                     time_window=500,
                                     func_expr="P90(sensor[0]) every 5 min",
                                     version="1.0",
                                     func_vars=["sample_size", "samples"])
        stage = Stage(execution_type=StageType.spark_sql,
                      output_type=StageOutputType.store,
                      sql_query="""
                      select cast(from_unixtime(floor(time/500)*500) as timestamp) as measure_time,
                      location_id, 
                      approx_percentile(data[0], 0.90) val,
                      count(1) sample_size,
                      collect_set(data[0]) samples
                      from input_data
                      group by 1,2
                      """,
                      horizontal_level=1,
                      vertical_level=0
                      )
        definition = Definition(source_metric).add_stage(stage)

        engine = get_engine(definition, spark, temp_dir)
        partitions = engine.compute(definition)
        self.assertEqual(len(partitions), 1)
        expected_keys = ['id', 'compute_date', 'compute_hour', 'horizontal_level',
                         'vertical_level']
        for partition_records in partitions.values():
            for row in partition_records:
                for k in expected_keys:
                    self.assertIn(k, row.asDict().keys())

    def test_python_stage(self):
        temp_dir = tempfile.mkdtemp()
        print("Temp dir {}".format(temp_dir))

        zip_filename = os.path.join(temp_dir, "test_lib.zip")
        with ZipFile(zip_filename, 'w') as zip_file:
            for folder, subfolders, filenames in os.walk("test/custom_test_stages"):
                for f in filenames:
                    source = os.path.join(folder, f)
                    arcname = f"custom_stages/{f}"
                    zip_file.write(filename=source, arcname=arcname)

        sys.path.append(zip_filename)

        source_metric = SourceMetric(metric_id="p90_sensor_0",
                                     time_window=500,
                                     func_expr="P90(sensor[0]) every 5 min",
                                     version="1.0",
                                     func_vars=["sample_size", "samples"])

        compute_stage = Stage(
            execution_type=StageType.python,
            output_type=StageOutputType.view,
            py_mod="custom_stages.sample_stage",
            py_stage_args={'lookback_window': 5, 'start_date': datetime(2020, 1, 10, 0, 3)}
        )
        definition = Definition(source_metric).add_stage(compute_stage)

        storage_stage = Stage(execution_type=StageType.spark_sql,
                              output_type=StageOutputType.store,
                              sql_query="""
                      select * from test_metric
                      """,
                              horizontal_level=1,
                              vertical_level=0
                              )

        definition.add_stage(storage_stage)

        conf = SparkConf().setAppName("unit_test").setMaster("local[2]"). \
            set("spark.submit.pyFiles", zip_filename)
        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        engine = get_engine(definition, spark, temp_dir)
        partitions = engine.compute(definition)
        self.assertEqual(len(partitions), 1)
        expected_keys = ['id', 'compute_date', 'compute_hour', 'horizontal_level',
                         'vertical_level']
        for partition_records in partitions.values():
            for row in partition_records:
                for k in expected_keys:
                    self.assertIn(k, row.asDict().keys())


def get_engine(definition, spark, temp_dir):
    reader = ctx.Reader(
        user=None,
        password=None,
        uri=None,
        reader_type=ctx.ReaderType.SPARK_NATIVE
    )
    writer = ctx.Writer(
        uri=temp_dir
    )
    context = ctx.Context(metric_definitions=[definition],
                          env="test",
                          metric_table="metrics",
                          metric_data_store="test",
                          reader=reader,
                          writer=writer,
                          compute_datetime=datetime.now()

                          )
    engine = SparkStandAlone(context=context, spark_session=spark)
    return engine


if __name__ == '__main__':
    unittest.main()
