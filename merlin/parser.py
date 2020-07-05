from typing import List

import yaml
from zipfile import ZipFile
from importlib import import_module

from merlin.metric import SourceMetric, Definition
from merlin.stage import Stage, StageType, StageOutputType


class MetricParserException(Exception):

    def __init__(self, message):
        super()


class MetricParser:

    def __init__(self):
        self.metric_fields = set(
            ['metric_id', 'time_window', 'func_expr', 'version'])

    def load_metrics(self, metric_db="merlin/yaml/metric_definition.yml") -> List[Definition]:

        definitions = []
        with open(metric_db, 'r') as file_handler:
            unparsed_definitions = yaml.load(
                file_handler, Loader=yaml.FullLoader)

            for d in unparsed_definitions:
                source_metric = self.parse_source_metric(d)
                if 'stages' not in d.keys():
                    raise MetricParserException("Stages not defined ")

                if not isinstance(d['stages'], list):
                    raise MetricParserException("Stages not a list ")

                metric_def = Definition(source_metric)

                for s in d['stages']:
                    metric_def.add_stage(self.get_stage(s))
                definitions.append(metric_def)

        return definitions

    def get_stage(self, s) -> Stage:
        stage_init_param = s.copy()
        stage_init_param['output_type'] = StageOutputType[s['output_type']]
        stage_init_param['stage_type'] = StageType[s['stage_type']]

        if s["stage_type"] == 'python':
            if "py_mod" not in s.keys():
                raise MetricParserException(
                    f"Incorrect YAML Definition: py_mod not found ")

        if s["stage_type"] == 'sql_query':
            if 'sql_query' not in s.keys():
                raise MetricParserException(
                    f"Incorrect YAML Definition: sql_query not found ")

            # TODO: find better location for sql zip
            with ZipFile("../test/sql.zip", 'r') as zip_file:
                query = zip_file.read(f"sql/{stage_init_param['sql_query']}")
                stage_init_param['sql_query'] = query

        return Stage(**stage_init_param)

    def parse_source_metric(self, source_map) -> SourceMetric:

        keys = set(source_map.keys())
        for k in self.metric_fields:
            if k not in keys:
                raise MetricParserException(
                    "Missing field {} in  {}".format(k, source_map))

        assert isinstance(source_map['metric_id'], str)
        assert isinstance(source_map['time_window'], int)
        assert isinstance(source_map['func_expr'], str)

        source_metric = SourceMetric(
            metric_id=source_map['metric_id'],
            time_window=source_map['time_window'],
            version=str(source_map['version']),
            func_expr=source_map['func_expr']
        )
        if 'func_vars' in keys:
            assert isinstance(source_map['func_vars'], list)
            source_metric.func_vars = source_map['func_vars']

        return source_metric
