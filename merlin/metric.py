#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# TODO add serializer and desirealiser from and to json

from datetime import datetime
from decimal import Decimal

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, ArrayType, \
    MapType

from merlin.stage import Stage
from merlin.values import StructuredValue


class Metric:
    """
    Basic definition of a metric
    """

    __slots__ = ['metric_id', 'time_window', 'func_expr', 'version']

    def __init__(self, metric_id: str, time_window: int, func_expr: str, version: str):
        """

        :param metric_id: identifier of the metric
        :param time_window: how often in second the metric is computed
        :param fun_expr:
        :param horizontal_level:
        :param vertical_level:

        """

        self.metric_id = metric_id
        self.time_window = time_window
        self.func_expr = func_expr
        self.version = version

    def __str__(self):
        d = {s: self.__getattribute__(s) for s in self.__slots__}
        return "{}".format(d)


class OutputMetric(Metric):
    __slots__ = ['group_keys', 'group_map', 'func_var', 'measure_time',
                 'compute_time', 'val', 'func_vars', 'horizontal_level',
                 'vertical_level']

    SPARK_TYPE = MapType(StringType(), StructType([
        StructField("type", StringType(), True),
        StructField("long", LongType(), True),
        StructField("double", DoubleType(), True),
        StructField("bool", BooleanType(), True),
        StructField("string", StringType(), True),
        # List <Object> in spark
        StructField("vec_long", ArrayType(LongType()), True),
        StructField("vec_double", ArrayType(DoubleType()), True),
        StructField("vec_bool", ArrayType(BooleanType()), True),
        StructField("vec_string", ArrayType(StringType()), True),
        # Maps <String, Object >
        StructField("map_long", MapType(StringType(), LongType()), True),
        StructField("map_double", MapType(StringType(), DoubleType()), True),
        StructField("map_bool", MapType(StringType(), BooleanType()), True),
        StructField("map_string", MapType(StringType(), StringType()), True),
    ]), True)

    SPARK_OUTPUT_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("measure_time", LongType(), True),
        StructField("compute_time", LongType(), True),
        StructField("compute_date", StringType(), True),
        StructField("compute_hour", LongType(), True),
        StructField("time_window", LongType(), True),
        StructField("val", DoubleType(), True),
        StructField("group_map", SPARK_TYPE, True),
        StructField("group_keys", ArrayType(StringType()), True),
        StructField("func_expr", StringType(), True),
        StructField("func_vars", SPARK_TYPE, True),
        StructField("horizontal_level", LongType(), True),
        StructField("vertical_level", LongType(), True),
        StructField("version", StringType(), True),

    ])

    def __init__(self, metric: Metric, val: float, measure_time: datetime, horizontal_level: int,
                 vertical_level: int):
        """
        :param val: value (float)
        :param measure_time: time when the metric was computed

        Note that compute time is initialised automatically but it can be overwritten
        """

        super().__init__(metric.metric_id, metric.time_window, metric.func_expr, metric.version)

        self.measure_time = to_datetime(measure_time)
        self.compute_time = datetime.now()
        self.val = float(val)
        self.group_keys = []
        self.group_map = {}
        self.func_vars = {}
        self.horizontal_level = horizontal_level
        self.vertical_level = vertical_level

    def add_group_value(self, value: StructuredValue):
        ##TODO Warn if same value is added twice
        self.group_map[value.value_id] = value

    def add_func_var(self, value: StructuredValue):
        ##TODO Warn if same value is added twice

        self.func_vars[value.value_id] = value

    def asdict(self):
        out_dict = {'id': self.metric_id,
                    'measure_time': int(self.measure_time.timestamp()),
                    'compute_time': int(self.compute_time.timestamp()),
                    'compute_date': self.compute_time.strftime("%Y-%m-%d"),
                    'compute_hour': self.compute_time.hour,
                    'time_window': self.time_window,
                    'val': self.val,
                    'group_map': {k: v.asdict() for k, v in self.group_map.items()},
                    'group_keys': list(self.group_map.keys()),
                    'func_vars': {k: v.asdict() for k, v in self.func_vars.items()},
                    'func_expr': self.func_expr,
                    'horizontal_level': self.horizontal_level,
                    'vertical_level': self.vertical_level,
                    'version': self.version
                    }

        return out_dict


class SourceMetric(Metric):
    __slots__ = ['metric_id', 'time_window', 'func_expr', 'version', 'func_vars']

    def __init__(self, metric_id: str, time_window: int, func_expr: str,
                 version: str, func_vars=[]):
        """

        :param metric_id: identifier of the metric
        :param time_window: how often in second the metric is computed
        :param func_expr:
        :param version: version identifier
        :param func_vars: list of strings for the functional variables


        """

        super().__init__(metric_id, time_window, func_expr, version)
        self.func_vars = func_vars


class Definition:
    """
    Simple container for metrics
    """
    metric: SourceMetric

    def __init__(self, metric: SourceMetric):
        self.stages = []
        self.metric = metric

    def add_stage(self, stage: Stage):
        self.stages.append(stage)
        return self

    def __repr__(self):
        return "metric:{}, stages[]".format(self.metric, ",".join([str(s) for s in self.stages]))

    def __str__(self):
        return "metric:{}, stages[]".format(self.metric, ",".join([str(s) for s in self.stages]))


def to_datetime(time) -> datetime:
    raw_type = type(time)

    if raw_type is datetime:
        return time

    if raw_type in [int, float, Decimal]:
        return datetime.fromtimestamp(time)

    raise (Exception("Un-parsable time {} of type {}".format(time, raw_type)))
