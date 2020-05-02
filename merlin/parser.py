import json

from merlin.metric import SourceMetric


class MetricParser():
    def __init__(self):
        return

    @staticmethod
    def load_metrics(metric_db="json/metric_definitions.json"):
        parsed = {}
        with open(metric_db, 'r') as file_handler:
            metrics_list = json.load(file_handler)
            if metrics_list is None:
                return parsed

            for object_map in metrics_list:
                parsed[object_map['id']] = object_map
        # TODO: use set to check if the dictionary is correct and ready for the source metrics

        return [SourceMetric(**parsed[metric]) for metric in parsed]