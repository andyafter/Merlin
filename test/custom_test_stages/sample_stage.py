import merlin.base_custom_stage as bcs


class SamplePythonStage(bcs.BaseCustomStage):

    def __init__(self, **kwargs):
        self.lookback_window = kwargs.get('lookback_window', None)
        self.start_date = kwargs.get('start_date', None)

    def run(self, context, spark_session, stage, metric_definition):
        spark = spark_session
        test_df = spark.read.json("test/data/synthetic_data.json.gz")
        test_df.createOrReplaceTempView("input_data")
        df = spark.sql("""select cast(from_unixtime(floor(time/500)*500) as timestamp) 
                    as measure_time,
                      location_id, 
                      approx_percentile(data[0], 0.90) val,
                      count(1) sample_size,
                      collect_set(data[0]) samples
                      from input_data
                      group by 1,2
                    """)
        df.createOrReplaceTempView("test_metric")


class Loader(bcs.Loader):

    def stage_class(self):
        return SamplePythonStage
