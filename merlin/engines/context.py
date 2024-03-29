from datetime import datetime
from enum import Enum
from typing import List
from urllib.parse import urlparse

from merlin.metric import Definition


class ReaderType(Enum):
    PRESTO = 0
    SPARK_NATIVE = 1
    JDBC = 2
    BIGQUERY = 3


class BigQueryReaderOption(Enum):
    WORKSPACE_PROJECT = "workspace_project"


class Reader:
    """
    Where we read the data from
    """

    __slots__ = ["user", "password", "uri", "type", 'driver', 'client', 'options']

    def __init__(self, user: str, password: str, uri: str, reader_type: ReaderType, driver=None, client=None,
                 options={}):
        """

        :param user: username to use for JDBC
        :param password:  password to use for JDBC
        :param uri:   either jdbc:// or file:// or s3://
        :param reader_type: ReaderType
        :param driver: class to load when loading the driver (Java class)
        """
        self.user = user
        self.password = password
        self.uri = urlparse(uri)
        self.type = reader_type
        self.driver = driver
        self.client = client
        self.options = options


class Writer:
    """"
    Where we write the data to
    """

    def __init__(self, uri):
        """

        :param uri: URI for engine to write the data to
        """
        self.uri = urlparse(uri)


class Store:
    """
    Where to store metrics and cache
    """

    def __init__(self, metrics: str, cache: str):
        """
        :param  metrics: table to store metrics
        :param  cache: views to cache
        """
        self.metrics = metrics
        self.cache = cache


class Context:
    """
    The context of the engine
    """

    __slots__ = ["metric_definitions", "env", "compute_datetime", "metric_table",
                 "metric_data_store", "reader", "writer", "store"]

    def __init__(self, metric_definitions: List[Definition], env: str,
                 compute_datetime: datetime,
                 metric_table: str,
                 metric_data_store: str,
                 reader: Reader,
                 writer: Writer,
                 store: Store
                 ):
        """

        :param metric_definitions: list of definition of the metrics
        :param env:  enviroment
        :param compute_datetime: time we compute stuff
        :param engine:  engine we should be using
        :param metric_table:  table where we store metric (metadata for hive)
        :param metric_data_store:  actual data store

        """
        self.metric_definitions = metric_definitions
        self.compute_datetime = compute_datetime
        self.env = env
        self.metric_table = metric_table
        self.metric_data_store = urlparse(metric_data_store)
        self.reader = reader
        self.writer = writer
        self.store = store
