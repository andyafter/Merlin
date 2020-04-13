import logging

class Logger:
    __instance = None

    @staticmethod
    def get_or_create(name, level):
        if Logger.__instance is None:
            logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s %(message)s', level=level)
            Logger.__instance = logging.getLogger(name)
            Logger.__instance.info(f"Init logger {name} with level {level}")
        return Logger.__instance


def get_logger(level="INFO", name="root"):
    return Logger.get_or_create(name=name, level=level)
