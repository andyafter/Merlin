from pyspark.sql import SparkSession

from merlin.engines.context import Context
from merlin.engines.engine import AbstractEngine
from merlin.metric import Definition
from merlin.stage import Stage, StageType
from merlin.utils import timed


class SparkStandAlone(AbstractEngine):

    def __init__(self, context: Context, spark_session: SparkSession):
        self.spark = spark_session
        self.context = context

    @timed
    def compute(self, metric_definition: Definition):
        self.LOGGER.info("Computing metric {} ".format(metric_definition))

        stages = metric_definition.stages
        stored_partitions = {}
        for stage in stages:

            if stage.execution_type == StageType.spark_sql:
                stored_partitions[stage.id] = self.process_sql_stage(stage, metric_definition)
            else:
                self.LOGGER("I don't know how to compute %s stage", stage.execution_type)

        return stored_partitions

    @timed
    def process_sql_stage(self, stage: Stage, definition: Definition):

        stage_df = self.spark.sql(stage.sql_query)
        stored_partitions = []

        if stage.is_store():
            stage_df.cache()
            size = stage_df.count()
            self.LOGGER.info("Writing to %d ", size)
            # TODO change compute suitable values for repartitioning
            output_df = self.to_metric_schema(stage_df, stage, definition, 2, 500)

            self.LOGGER.info("Writing to %s ", self.context.writer.uri)
            # output_df.write.partitionBy(*self.DEFAULT_PARTITION_COLUMNS). \
            #     format("orc"). \
            #     save(self.context.writer.uri)
            output_df.show(100, 100)

            partitions = output_df.select(*self.DEFAULT_PARTITION_COLUMNS).distinct().collect()
            stored_partitions.extend(partitions)

        elif stage.is_view():
            stage_df.createOrReplaceTempView(stage.view_name)

        return stored_partitions
