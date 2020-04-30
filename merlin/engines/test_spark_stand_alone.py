import unittest
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession

import merlin.engines.context as ctx
from merlin.engines.spark_stand_alone import SparkStandAlone
from merlin.metric import SourceMetric, Definition
from merlin.stage import Stage, StageType, StageOutputType


class MyTestCase(unittest.TestCase):

    def test_engine(self):
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

        reader = ctx.Reader(
            user=None,
            password=None,
            uri=None,
            reader_type=ctx.ReaderType.SPARK_NATIVE
        )

        writer = ctx.Writer(
            uri="file://tmp/metrics"
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
        engine.compute(definition)


if __name__ == '__main__':
    unittest.main()
