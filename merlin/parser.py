import json

from merlin.metric import SourceMetric


class MetricParser():
    def __init__(self):
        return

    @staticmethod
    def load_metrics(metric_db="yaml/metric_definition.yml"):

        parsed = {}
        with open(metric_db, 'r') as file_handler:
            metrics_list = json.load(file_handler)
            if metrics_list is None:
                return parsed

            for object_map in metrics_list:
                parsed[object_map['id']] = object_map
        # TODO: use set to check if the dictionary is correct and ready for the source metrics
        # when reading decide which stage it is

        return [SourceMetric(**parsed[metric]) for metric in parsed]