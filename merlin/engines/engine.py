from abc import ABC

from merlin.logger import get_logger
from merlin.metric import Definition

LOGGER = get_logger(level="INFO", name=__name__)


class AbstractEngine(ABC):

    @classmethod
    def compute(cls, metric_definition: Definition):
        pass


class NullOpsEngine(AbstractEngine):

    def __init__(self, context, spark):
        LOGGER.info("NullOpsEngine started")
        LOGGER.info(context)

    def compute(self, metric_definition: Definition):
        LOGGER.info("NullOpsEngine does not execute")
        LOGGER.info(metric_definition)
