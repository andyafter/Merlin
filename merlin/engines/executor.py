from enum import Enum
from multiprocessing.pool import ThreadPool

from pyspark import SparkConf
from pyspark.sql import SparkSession

from merlin.engines.context import Context
from merlin.engines.engine import NullOpsEngine
from merlin.logger import get_logger
from merlin.metric import Definition

LOGGER = get_logger(level="INFO", name=__name__)


class ExecutionType(Enum):
    SPARK_STAND_ALONE = 0
    SPARK_PRESTO = 1
    SPARK_BIG_QUERY = 2


class Executor:
    """
    This is the ECU of all engine:
    - Starts the engine
    - Controls the engines
    - Register output they produced
    """
    spark_session: SparkSession

    def __init__(self, context: Context, execution_type: ExecutionType, spark_session=None, spark_conf={},
                 app_name="Merlin", concurrency=1):
        self.context = context
        self.execution_type = execution_type
        self.spark_session = spark_session
        self.spark_conf = spark_conf
        self.app_name = app_name
        self.concurrency = concurrency
        self.engine_results = []
        self.__create_session__()
        self.__initialize_engine__()
        self.pool = ThreadPool(processes=self.concurrency)

    def __create_session__(self):
        """
        Create a spark session if it is None
        :return:
        """
        if self.spark_session is None:
            conf = SparkConf().setAppName(self.app_name).set("spark.scheduler.mode", "FAIR")
            for k, v in self.spark_conf.items():
                LOGGER("Setting spark.conf {} => {}".format(k, v))
                conf.set(k, v)

            self.spark_session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    def __initialize_engine__(self):

        if self.execution_type == ExecutionType.SPARK_STAND_ALONE:
            pass
        elif self.execution_type == ExecutionType.SPARK_PRESTO:
            pass
        elif self.execution_type == ExecutionType.SPARK_BIG_QUERY:
            pass
        else:
            self.engine = NullOpsEngine(self.context, self.spark_session)

    def compute_all(self):
        metric_definitions = self.context.metric_definitions
        LOGGER.info("Starting computing metrics from {} definitions".format(len(metric_definitions)))
        engine_results = self.pool.map(lambda metric: self.compute_metric(metric), metric_definitions)
        self.engine_results.extend(engine_results)
        return engine_results

    def compute_metric(self, metric_definition: Definition):

        metric_df = self.engine.compute(metric_definition)

    def create_or_update_partitions(self, metric_df):
        """ update the hive partitions"""
        pass

    def exit(self):
        self.spark_session.stop()
