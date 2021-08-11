import importlib
import uuid
from enum import Enum

from merlin.base_custom_stage import BaseCustomStage
from merlin.logger import get_logger

LOGGER = get_logger(level="INFO", name=__name__)


class StageType(Enum):
    presto_sql = 0
    spark_sql = 1
    python = 2
    big_query = 3


class StageOutputType(Enum):
    view = 0
    store = 1
    cached_view = 2


class StageException(Exception):

    def __init__(self, message):
        super()


class Stage:
    horizontal_level: int
    vertical_level: int
    region: str

    def __init__(self, stage_type: StageType, output_type=StageOutputType.view,
                 stage_id=str(uuid.uuid4()), sql_query=None, horizontal_level=0, vertical_level=0,
                 view_name=None, py_mod=None, py_stage_args=None, region=None, workspace_dataset=None, **kwargs):
        self.id = stage_id
        self.stage_type = stage_type
        self.sql_query = sql_query
        self.output_type = output_type
        self.horizontal_level = horizontal_level
        self.vertical_level = vertical_level
        self.view_name = view_name
        self.py_mod = py_mod
        self.py_stage_args = py_stage_args
        self.validate()
        self.region = region
        self.workspace_dataset = workspace_dataset

        if self.stage_type == StageType.python:
            self.py_stage = self.__load_custom_stage__()
        else:
            self.py_stage = None

    def is_view(self):
        return self.output_type == StageOutputType.view or self.output_type == StageOutputType.cached_view

    def is_store(self):
        return self.output_type == StageOutputType.store

    def validate(self):
        """
       1 . validate data types
       2.  if stage is python  => py_stage and
        :return:
        """
        if self.stage_type not in StageType:
            raise StageException("StageType inconsistent ")

        if self.stage_type == StageType.python:
            if self.py_mod is None:
                raise StageException("PyMod cannot be None if stage type is Python ")
        else:
            if self.sql_query is None:
                raise StageException("Sql Query cannot be None if stage type is not Python ")

        self.validate_python_stage()

    def validate_python_stage(self):
        """
        Validate loading that py_stage is not none
        :return:
        """
        if self.stage_type == StageType.python:
            assert self.py_mod is not None

    def __load_custom_stage__(self) -> BaseCustomStage:
        """
        Load a stage definition form a python file
        :param name of python modules:
        :return:
        """
        stage_definition = importlib.import_module(self.py_mod)
        stage_class = stage_definition.Loader().stage_class()

        if self.py_stage_args is None:
            stage_instance = stage_class.run()
        else:
            stage_instance = stage_class.run(**self.py_stage_args)

        return stage_instance

    def __str__(self):
        return self.id
