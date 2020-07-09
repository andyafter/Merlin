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


def get_engine(engine_type, definitions, spark, options, configs):
    context = ctx.Context(metric_definitions=definitions,
                          env="test",
                          metric_table="metrics",
                          metric_data_store="test",
                          reader=None,
                          writer=None,
                          compute_datetime=datetime.now()

                          )

    if engine_type == "spark_stand_alone":
        context.reader = ctx.Reader(
            user=None,
            password=None,
            uri=None,
            reader_type=ctx.ReaderType.SPARK_NATIVE
        )
        context.writer = ctx.Writer(
            uri=configs["bucket"] if "writer" in configs and "bucket" in configs["writer"] else None
        )
        engine = SparkStandAlone(context=context, spark_session=spark)

    elif engine_type == "big_query":
        context.reader = ctx.Reader(
            user=None,
            password=None,
            uri=None,
            client=None,  # Pass the client if the keyfile is not null
            reader_type=ctx.ReaderType.BIGQUERY,
            options=options
        )
        engine = SparkBigQuery(context=context, spark_session=spark)

    else:
        raise Exception("Unsupported engine")

    return engine


def bool_parse(arg: str):
    return arg.lower() == "true"


def get_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Metric Engine")
    parser.add_argument("--metric_db", help="Metric database",
                        type=str, nargs="?", required=True)
    parser.add_argument("--sql_archive", help="SQL definition zip",
                        type=str, nargs="?", required=True)
    parser.add_argument("--pymod_archive", help="SQL definition zip",
                        type=str, nargs="?", required=False)
    parser.add_argument("--keyfile", type=str, nargs="?", required=False)
    parser.add_argument("--configs", type=str, nargs="?", required=False)
    parser.add_argument("--dt_start", type=str, nargs="?", required=True)
    parser.add_argument("--dt_end", type=str, nargs="?", required=True)
    parser.add_argument("--gcs_temp_bucket", type=str,
                        nargs="?", required=False)
    parser.add_argument("--k8s_chdir", type=bool_parse,
                        required=False, default="False")
    parser.add_argument("--engine", type=str, required=False, default="False")

    return parser


def get_spark_session(keyfile=None, chdir=False, gcs_temp_bucket=None) -> SparkSession:
    conf = SparkConf().setAppName("Metric Engine").set("spark.scheduler.mode", "FAIR")

    if gcs_temp_bucket is not None:
        conf = conf.set('temporaryGcsBucket', gcs_temp_bucket)
        conf = conf.set("spark.hadoop.fs.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        conf = conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        conf = conf.set(
            'spark.hadoop.fs.gs.auth.service.account.enable', 'true')

    if keyfile is not None:
        conf = conf.set(
            'spark.hadoop.google.cloud.auth.service.account.json.keyfile', keyfile)

    spark = SparkSession.builder.config(
        conf=conf).enableHiveSupport().getOrCreate()
    if chdir:
        os.chdir(SparkFiles.getRootDirectory())
    return spark


def load_definition(metric_db, sql_archive, pymod_archive):
    parser = MetricParser(metric_db, sql_archive, pymod_archive)
    definitions = parser.load_metrics()
    return definitions


def get_configs(file_name):
    with open(file_name, 'r') as fh:
        configs = yaml.load(fh, Loader=yaml.FullLoader)
    return configs


if __name__ == '__main__':
    args = get_argparser().parse_args()
    spark = get_spark_session(
        args.keyfile, args.k8s_chdir, args.gcs_temp_bucket)
    definitions = load_definition(
        args.metric_db, args.sql_archive, args.pymod_archive)

    spark_session = get_spark_session()

    configs = get_configs(args.configs)

    options = {
        'keyfile': args.keyfile
    }
    engine = get_engine(args.engine, definitions,
                        spark_session, options, configs["stg"])

    for metric_def in definitions:
        partitions = engine.compute(metric_def)

        expected_keys = ['id', 'compute_date', 'compute_hour', 'horizontal_level',
                         'vertical_level']
        for partition_records in partitions.values():
            for row in partition_records:
                print(row)

        spark_session.stop()

# parser yaml -> create metric -> run stage
