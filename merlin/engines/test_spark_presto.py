import tempfile
import unittest
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession

import merlin.engines.context as ctx
from merlin.engines.spark_presto import SparkPresto
from merlin.metric import SourceMetric, Definition
from merlin.stage import Stage, StageType, StageOutputType


class SparkPrestoTestCase(unittest.TestCase):

    def test_engine(self):
        temp_dir = tempfile.mkdtemp()
        print("Temp dir {}".format(temp_dir))
        conf = SparkConf().setAppName("unit_test"). \
            setMaster("local[2]"). \
            set("spark.jars", "test/jars/sqlite-jdbc-3.30.1.jar")
        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

        source_metric = SourceMetric(metric_id="max_sensor_0",
                                     time_window=500,
                                     func_expr="max(sensor[0]) every 5 min",
                                     version="1.0",
                                     func_vars=["sample_size", "min_val"])
        stage = Stage(execution_type=StageType.presto_sql,
                      output_type=StageOutputType.store,
                      sql_query="""
                      select cast(time/500 as INTEGER)*500 as measure_time,
                      cast(methane_id as INTEGER) methane_id, 
                      cast(max(data) as REAL) val,
                      cast(count(1) as INTEGER) sample_size,
                      cast(min(data) as REAL) min_val
                      from input_data
                      group by 1,2
                      """,
                      horizontal_level=1,
                      vertical_level=0
                      )
        definition = Definition(source_metric).add_stage(stage)

        reader = ctx.Reader(
            user="test-user",
            password=None,
            driver="org.sqlite.JDBC",
            uri="jdbc:sqlite:test/data/synthetic_data.sqlite",
            reader_type=ctx.ReaderType.PRESTO
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

        engine = SparkPresto(context=context, spark_session=spark)
        partitions = engine.compute(definition)
        self.assertEqual(len(partitions), 1)
        expected_keys = ['id', 'compute_date', 'compute_hour', 'horizontal_level',
                         'vertical_level']
        for partition_records in partitions.values():
            for row in partition_records:
                for k in expected_keys:
                    self.assertIn(k, row.asDict().keys())


if __name__ == '__main__':
    unittest.main()
