from typing import List
from zipfile import ZipFile

import yaml

from merlin.metric import SourceMetric, Definition
from merlin.stage import Stage, StageType, StageOutputType


class MetricParserException(Exception):

    def __init__(self, message):
        super()


class MetricParser:
    METRIC_FIELDS = set(['metric_id', 'time_window', 'func_expr', 'version'])

    def __init__(self, metric_db: str, query_definition_zip: str, python_mod_zip: str):
        self.metric_db = metric_db
        self.query_definition_zip = query_definition_zip
        self.python_mod_zip = python_mod_zip
        # TODO handle mods

    @staticmethod
    def validate_definition_dict(d):

        if 'stages' not in d.keys():
            raise MetricParserException("Stages not defined ")

        if not isinstance(d['stages'], list):
            raise MetricParserException("Stages not a list ")

    def load_metrics(self) -> List[Definition]:
        definitions = []
        with ZipFile(self.metric_db, 'r') as zip_file:
            metrics = {name: zip_file.read(name) for name in list(zip_file.namelist())}

        for m in metrics:
            unparsed_definitions = [yaml.load(metrics[m], Loader=yaml.FullLoader)]
            definitions = definitions + self.parse_definitions(unparsed_definitions)

        return definitions

    def parse_definitions(self, unparsed_definitions: List[dict]):
        definitions = []
        for d in unparsed_definitions:
            self.validate_definition_dict(d)

            source_metric = self.parse_source_metric(d)
            metric_def = Definition(source_metric)

            for s in d['stages']:
                metric_def.add_stage(self.load_stage(s))

            definitions.append(metric_def)
        return definitions

    def load_query_from_zip(self, s):
        if 'sql_query' not in s.keys():
            raise MetricParserException(
                f"Incorrect YAML Definition: sql_query not found ")
        with ZipFile(self.query_definition_zip, 'r') as zip_file:
            query = zip_file.read(f"sql/{s['sql_query']}")
        return query.decode("utf-8")

    def load_stage(self, s) -> Stage:
        stage_init_param = s.copy()

        if s["stage_type"] == StageType.python.name:
            if "py_mod" not in s.keys():
                raise MetricParserException(
                    f"Incorrect YAML Definition: py_mod not found ")

        if s["stage_type"] in [StageType.presto_sql.name, StageType.big_query.name, StageType.spark_sql.name]:
            query = self.load_query_from_zip(s)
            stage_init_param['sql_query'] = query

        stage_init_param['output_type'] = StageOutputType[s['output_type']]
        stage_init_param['stage_type'] = StageType[s['stage_type']]

        return Stage(**stage_init_param)

    def parse_source_metric(self, source_map) -> SourceMetric:

        keys = set(source_map.keys())
        for k in self.METRIC_FIELDS:
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
