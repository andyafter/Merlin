from merlin.base_custom_stage import BaseCustomStage


class CustomStage(BaseCustomStage):
    """
    Default costom stage is supposed to be acted as a default agent if any python module was not successfully loaded
    """

    def __init__(self):
        print("initialized default python module")

    def __call__(self):
        self.run()

    def run(self):
        print("running default python module")
        pass
