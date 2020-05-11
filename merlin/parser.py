import yaml

from merlin.metric import SourceMetric
from merlin.utils import LOGGER

class MetricParser():
    def __init__(self):
        # the metric_fields is a set of metric field strings. This is for validation use.
        self.metric_fields = set(['id', 'time', 'w_time', 'group_map', 'group_keys', 'func_map', 'func_expr', 'func_vars', 'compute_time', 'v_lvl', 'h_lvl', 'stages', 'description', 'version'])
        return

    def load_metrics(self, metric_db="merlin/yaml/metric_definition.yml"):

        parsed = {}
        with open(metric_db, 'r') as file_handler:
            parsed = yaml.load(file_handler, Loader=yaml.FullLoader)
            keys = set(parsed.keys())

            if not keys.issubset(self.metric_fields):
                LOGGER.info("Error: wrong metric field in definition.")
                return None

        m = SourceMetric(parsed['id'], parsed['w_time'], parsed['func_expr'], parsed['version'])

        return m