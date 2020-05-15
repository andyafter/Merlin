from test.modules.BasicTest.custom_stage import CustomStage


class Loader:

    def __init__(self):
        pass

    @staticmethod
    def stage_class(self):
        return CustomStage()