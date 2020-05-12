from typing import List

import yaml

from merlin.metric import SourceMetric, Definition
from merlin.stage import Stage, StageType, StageOutputType


class MetricParserException(Exception):

    def __init__(self, message):
        super()


class MetricParser:

    def __init__(self):
        self.metric_fields = set(['metric_id', 'time_window', 'func_expr', 'version'])

    def load_metrics(self, metric_db="merlin/yaml/metric_definition.yml") -> List[Definition]:

        definitions = []
        with open(metric_db, 'r') as file_handler:
            unparsed_definitions = yaml.load(file_handler, Loader=yaml.FullLoader)

            for d in unparsed_definitions:
                source_metric = self.parse_source_metric(d)

                if 'stages' not in d.keys():
                    raise MetricParserException("Stages not defined ")

                if not isinstance(d['stages'], list):
                    raise MetricParserException("Stages not a list ")

                metric_def = Definition(source_metric)

                for s in d['stages']:
                    stage_init_param = s.copy()
                    if 'sql_query' in s.keys():
                        # TODO load sql from file
                        # stage_init_param['sql_query'] = actual file content
                        pass
                    stage_init_param['output_type'] = StageOutputType[s['output_type']]
                    stage_init_param['stage_type'] = StageType[s['stage_type']]
                    metric_def.add_stage(Stage(**stage_init_param))

        return definitions

    def parse_source_metric(self, source_map) -> SourceMetric:

        keys = set(source_map.keys())
        for k in self.metric_fields:
            if k not in keys:
                raise MetricParserException("Missing field {} in  {}".format(k, source_map))

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
