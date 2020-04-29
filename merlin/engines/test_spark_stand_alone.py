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
        rdd = spark.sparkContext.parallelize([{'mass': 100.0, 'density': 40.0, 'time': 15140210}])
        test_df = rdd.toDF()
        test_df.createOrReplaceTempView("input_table")
        spark.sql("select * from input_table").show(100, 100)
        source_metric = SourceMetric(metric_id="volumetric_flow_rate",
                                     time_window=500,
                                     func_expr="mass / density / time",
                                     horizontal_level=1,
                                     vertical_level=0,
                                     version="1.0",
                                     func_vars=["mass", "density", "time"])
        stage = Stage(execution_type=StageType.spark_sql,
                      output_type=StageOutputType.store,
                      sql_query="select cast(count(1) as double) as val, current_timestamp as measure_time from input_table "
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
