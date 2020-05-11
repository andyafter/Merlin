import re
from abc import ABC

from pyspark.sql import DataFrame, Row

from merlin.logger import get_logger
from merlin.metric import Definition, OutputMetric
from merlin.stage import Stage
from merlin.values import StructuredValue

FUNCTIONAL_VARIABLE_NAME_PREFIX = re.compile(r"^func_var_")


class AbstractEngine(ABC):
    LOGGER = get_logger(level="INFO", name=__name__)
    DEFAULT_PARTITION_COLUMNS = ["id", "compute_date", "compute_hour", "horizontal_level", "vertical_level"]

    @classmethod
    def compute(cls, metric_definition: Definition):
        pass

    def to_metric_schema(self, data_frame: DataFrame, stage: Stage,
                         definition: Definition, output_partition: int,
                         repartition_count: int) -> DataFrame:
        out_rdd = data_frame.repartition(repartition_count).rdd.map(lambda row: row_mapper(row, stage,
                                                                                           definition))
        out_df = out_rdd.toDF(OutputMetric.SPARK_OUTPUT_SCHEMA).repartition(output_partition)
        return out_df


def row_mapper(row: Row, stage: Stage, definition: Definition) -> dict:
    managed_cols = ['val', 'measure_time']
    val = row['val']
    measure_time = row['measure_time']

    output_metric = OutputMetric(definition.metric, val=val,
                                 measure_time=measure_time,
                                 horizontal_level=stage.horizontal_level,
                                 vertical_level=stage.vertical_level
                                 )

    functional_variables = definition.metric.func_vars.copy()
    row_dict = row.asDict()
    columns = row_dict.keys()

    for col in columns:
        if FUNCTIONAL_VARIABLE_NAME_PREFIX.match(col) and col not in functional_variables:
            functional_variables.append(col)

    for col in functional_variables:
        func_key = FUNCTIONAL_VARIABLE_NAME_PREFIX.sub('', col)
        if col in row_dict.keys():
            output_metric.add_func_var(StructuredValue(row[col], func_key))

    managed_cols.extend(functional_variables)
    group_map_keys = row_dict.keys() - managed_cols

    for col in group_map_keys:
        output_metric.add_group_value(StructuredValue(row[col], col))

    return output_metric.asdict()


class NullOpsEngine(AbstractEngine):

    def __init__(self, context, spark):
        self.LOGGER.info("NullOpsEngine started")
        self.LOGGER.info(context)

    def compute(self, metric_definition: Definition):
        self.LOGGER.info("NullOpsEngine does not execute")
        self.LOGGER.info(metric_definition)
