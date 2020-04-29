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
        spark.sql("select * from input_data").show(10, 50)
        source_metric = SourceMetric(metric_id="volumetric_flow_rate",
                                     time_window=500,
                                     func_expr="mass / density / time",
                                     horizontal_level=1,
                                     vertical_level=0,
                                     version="1.0",
                                     func_vars=["mass", "density"])
        stage = Stage(execution_type=StageType.spark_sql,
                      output_type=StageOutputType.store,
                      sql_query="""
                      select cast(from_unixtime(floor(time/500)*500) as timestamp) as measure_time,
                      methane_id, 
                      max(data[0]) val,
                      collect_set(data[0]) func_var_samples
                      from input_data
                      group by 1,2
                      """
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
