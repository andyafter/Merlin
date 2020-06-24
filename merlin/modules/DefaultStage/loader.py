from test.modules.BasicTest.custom_stage import CustomStage


class Loader:

    def __init__(self):
        pass

    def stage_class(self):
        print("stage class called")
        return CustomStage()
