import json


class MetricParser():
    def __init__(self):
        return

    @staticmethod
    def load_metrics(metric_db="json/metric_definitions.json"):
        metrics = {}
        with open(metric_db, 'r') as file_handler:
            metrics_list = json.load(file_handler)
            if metrics_list is None:
                return metrics

            for object_map in metrics_list:
                metrics[object_map['id']] = object_map

        return metrics