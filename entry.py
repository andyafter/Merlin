'''spark entry for metric engine'''
import argparse
import os
from datetime import datetime

from pyspark import SparkConf, SparkFiles
from pyspark.sql import SparkSession
import yaml

import merlin.engines.context as ctx
from merlin.engines.spark_bigquery import SparkBigQuery
from merlin.engines.spark_stand_alone import SparkStandAlone
from merlin.parser import MetricParser


def get_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Metric Engine")
    parser.add_argument("--metric_definitions", help="Metric database",
                        type=str, nargs="?", required=True)

    return parser


if __name__ == '__main__':
    args = get_argparser().parse_args()
    with open(args.metric_definitions, 'r') as file:
        metrics_definitions = yaml.load(file, Loader=yaml.FullLoader)

        print(metrics_definitions)
