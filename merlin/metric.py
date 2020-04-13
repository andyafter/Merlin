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

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType, ArrayType, \
    MapType
from merlin.values import ComposedValue
from merlin.stage import Stage

class Metric:
    """
    Basic definition of a metric
    """

    def __init__(self, id: str, w_time: int, fun_expr: str, h_lvl: int, v_lvl: int):
        self.id = id
        self.time = None
        self.w_time = w_time
        self.val = None
        self.group_map = {}
        self.group_keys = []
        self.fun_expr = fun_expr
        self.fun_vars = {}
        self.h_lvl = h_lvl
        self.v_lvl = v_lvl
        self.compute_time = None


class OutputMetric(object):
    __slots__ = ["id", "time", "w_time", "val", "h_lvl", "v_lvl",
                 "group_map",
                 "group_keys",
                 "func_expr",
                 "func_vars",
                 "compute_date",
                 "compute_hour"]

    SPARK_TYPE = MapType(StringType(), StructType([
        StructField("type", StringType(), True),
        StructField("int", LongType(), True),
        StructField("double", DoubleType(), True),
        StructField("bool", BooleanType(), True),
        StructField("string", StringType(), True),
        # List <Object> in spark
        StructField("i_vec", ArrayType(LongType()), True),
        StructField("d_vec", ArrayType(DoubleType()), True),
        StructField("b_vec", ArrayType(BooleanType()), True),
        StructField("s_vec", ArrayType(StringType()), True),
        # Maps <String, Object >
        StructField("i_map", MapType(StringType(), LongType()), True),
        StructField("d_map", MapType(StringType(), DoubleType()), True),
        StructField("b_map", MapType(StringType(), BooleanType()), True),
        StructField("s_map", MapType(StringType(), StringType()), True),
    ]), True)

    SPARK_OUTPUT_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("time", LongType(), True),
        StructField("w_time", LongType(), True),
        StructField("val", DoubleType(), True),
        StructField("h_lvl", LongType(), True),
        StructField("v_lvl", LongType(), True),
        StructField("group_map", SPARK_TYPE, True),
        StructField("group_keys", ArrayType(StringType()), True),
        StructField("func_expr", StringType(), True),
        StructField("func_vars", SPARK_TYPE, True),
        StructField("compute_date", StringType(), True),
        StructField("compute_hour", LongType(), True)
    ])

    def __init__(self):
        self.group_keys = []
        self.group_map = {}
        self.func_vars = {}

    def add_group_value(self, value:ComposedValue):
        ##TODO Warn if same value is added twice
        self.group_map[value.value_id] = value

    def add_func_var(self, value:ComposedValue):
        ##TODO Warn if same value is added twice

        self.func_vars[value.value_id] = value

    def asdict(self):
        return {'id': self.id,
                'time': int(self.time.timestamp()),
                'w_time': self.w_time,
                'val': self.val,
                'lvl': self.lvl,
                'group_map': {k:v.asdict() for k,v in self.group_map},
                'group_keys': self.group_map.keys(),
                'func_vars':  {k:v.asdict() for k,v in self.group_map},
                'func_expr': self.func_expr,
                'ingestion_date': self.ingestion_date,
                'ingestion_hour': self.ingestion_hour
                }


class SourceMetric(Metric):

    def __init__(self, id, w_time, fun_expr, fun_vars=[]):
        super().__init__(id, w_time, fun_expr)
        self.fun_vars = fun_vars


class Definition:
    """
    Simple container for metrics
    """
    def __init__(self, metric: SourceMetric):
        self.stages = []
        self.metric = metric

    def add_stage(self, stage:Stage):
        self.stages.append(stage)

