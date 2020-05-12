from abc import ABC


class BaseCustomStage(ABC):
    """
    Base class to implement for a custom python stage
    """

    @classmethod
    def __init__(cls, **kwargs):
        """
        How the stage is initialied **kwargs are passed through the
        initialization see Stage.load_custom_stage
        :param kwargs:
        """
        pass

    @classmethod
    def run(cls, context, spark_session, stage, metric_definition):
        """
        This method is invoked by the engine
        :param context:
        :param spark_session
        :param stage:
        :param metric_definition:
        :return:

        As general suggestion custom stage should only create views
        rather than store it and then use a simple "select * " in
        another stage so that the storage is standardized
        """
        pass


class Loader(ABC):
    """
    This class needs to be defined in your custom stage and must
    be name Loader.
    This is used to identify which class inside the module is
    the stage class
    """

    @classmethod
    def stage_class(cls) -> BaseCustomStage:
        """
        This method must be implemented in order to
        :return:
        """
        pass
