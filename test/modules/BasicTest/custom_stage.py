from merlin.base_custom_stage import BaseCustomStage


class CustomStage(BaseCustomStage):
    """
    Base class to implement for a custom python stage for testing
    """

    def __init__(self):
        print("initialized test python module")

    def run(self):
        print("running test python module")
        pass
