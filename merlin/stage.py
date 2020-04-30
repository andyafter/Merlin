import importlib
import uuid
from enum import Enum

from merlin.logger import get_logger

LOGGER = get_logger(level="INFO", name=__name__)


class StageType(Enum):
    presto_sql = 0
    spark_sql = 1
    python = 2


class StageOutputType(Enum):
    view = 0
    store = 1
    cached_view = 2


class Stage:

    def __init__(self, execution_type: StageType, output_type=StageOutputType.view,
                 stage_id=str(uuid.uuid4()), sql_query=None, horizontal_level=0, vertical_level=0,
                 view_name=None, py_mod=None, py_stage_args=None, engine_context=None):
        self.id = stage_id
        self.execution_type = execution_type
        self.sql_query = sql_query
        self.output_type = output_type
        self.horizontal_level = horizontal_level
        self.vertical_level = vertical_level
        self.view_name = view_name
        self.py_stage = py_mod
        self.py_stage_args = py_stage_args
        self.engine_context = engine_context
        self.validate()

        if self.execution_type == StageType.python.name:
            self.py_stage = self.load_custom_stage()

    def is_view(self):
        return self.output_type == StageOutputType.view or self.output_type == StageOutputType.cached_view

    def is_store(self):
        return self.output_type == StageOutputType.store

    def validate(self):
        self.validate_python_stage()

    def validate_python_stage(self):
        """
        Validate loading that py_stage is not none
        :return:
        """
        if self.execution_type == StageType.python:
            assert self.py_stage is not None

    def load_custom_stage(self):
        """
        Load a stage definition form a python file
        :param name of python modules:
        :return:
        """
        stage_definition = importlib.import_module(self.py_stage)
        stage_class = stage_definition.stage_class()

        if self.py_stage_args is None:
            stage_instance = stage_class()
        else:
            stage_instance = stage_class(**self.py_stage_args)

        stage_instance.engine_context = self.engine_context
        return stage_instance

    def __str__(self):
        return self.id
