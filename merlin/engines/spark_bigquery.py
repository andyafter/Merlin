from google.cloud import bigquery
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
        self.big_query_client = self._get_or_create_client()
        self.workspace_project = self.context.big_query.workspace_project

    def _get_or_create_client(self):
        # TODO check if the client exists in Context
        client = bigquery.Client()
        return client

    @timed
    def compute(self, metric_definition: Definition) -> dict:
        self.LOGGER.info("Computing metric {} ".format(metric_definition))

        stages = metric_definition.stages
        stored_partitions = {}
        for stage in stages:

            if stage.stage_type == StageType.spark_sql:
                stored_partitions[stage.id] = self.process_sql_stage(stage, metric_definition)
            elif stage.stage_type == StageType.python:
                self.process_python_stage(stage, metric_definition)
            elif stage.stage_type == StageType.big_query:
                self.process_big_query(stage, metric_definition)
            else:
                self.LOGGER("I don't know how to compute %s stage", stage.stage_type)

        return stored_partitions

    @timed
    def process_big_query_stage(self, stage: Stage, definition: Definition):
        # TODO handle uniques to make sure two run are fine
        destination_table_id = "{}.{}.{}".format(self.workspace_project, stage.workspace_dataset, stage.view_name)
        self.client.delete_table(destination_table_id, not_found_ok=True)
        job_config = bigquery.QueryJobConfig(destination=destination_table_id)
        query_job = self.client.query(stage.sql_query, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.
        # TODO check result
        df = self.spark.read.format("bigquery").option("table", destination_table_id).load()
        df.createOrReplaceTempView(stage.view_name)

    @timed
    def process_sql_stage(self, stage: Stage, definition: Definition) -> list:

        stage_df = self.spark.sql(stage.sql_query)
        stored_partitions = []

        if stage.is_store():
            stage_df.cache()
            size = stage_df.count()
            self.LOGGER.info("Writing to %d ", size)
            # TODO change compute suitable values for repartitioning
            output_df = self.to_metric_schema(stage_df, stage, definition, 2, 500)

            self.LOGGER.info("Writing to %s ", self.context.writer.uri.geturl())
            output_df.write.partitionBy(*self.DEFAULT_PARTITION_COLUMNS). \
                format("orc").mode("append"). \
                save(self.context.writer.uri.geturl())

            partitions = output_df.select(*self.DEFAULT_PARTITION_COLUMNS).distinct().collect()
            stored_partitions.extend(partitions)

        elif stage.is_view():
            stage_df.createOrReplaceTempView(stage.view_name)

        return stored_partitions

    @timed
    def process_python_stage(self, stage: Stage, definition: Definition):
        stage.py_stage.run(self.context, self.spark, stage, definition)
