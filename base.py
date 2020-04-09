import abc
import json
from enum import Enum


class AtomicValueException(Exception):

    def __init__(self):
        pass


class AtomicTypes(Enum):
    INT = 1
    DOUBLE = 2
    BOOL = 3
    STRING = 4


class AtomicValue:
    ACCEPTED_PYTHON_TYPES = set([int, float, bool, str])
    MAPPED_TYPES = {int: ('int', AtomicTypes.INT),
                    float: ('float', AtomicTypes.DOUBLE),
                    bool: ('bool', AtomicTypes.BOOL),
                    str: ('string', AtomicTypes.STRING)
                    }

    __slots__ = ['int', 'double', 'bool', 'string', 'type', '__raw_type__']

    def __init__(self, value):
        self.__raw_type__ = type(value)

        if self.is_atomic(self.__raw_type__) is False:
            raise AtomicValueException()

        self.__set_value__(value)

    def __set_value__(self, attr_value):
        attr_name, self.type = self.MAPPED_TYPES[self.__raw_type__]
        self.__setattr__(attr_name, attr_value)

    @staticmethod
    def is_atomic(raw_type):
        return raw_type in AtomicValue.ACCEPTED_PYTHON_TYPES


class OutputValue:
    __slots__ = ['single_value', 'list_value', 'map_value', 'type', '__input_value__']

    def __init__(self, input_value):
        self.__input_value__ = input_value
        raw_type = type(input_value)

        if AtomicValue.is_atomic(raw_type):
            self.single_value = AtomicValue(self.__input_value__)
            self.type = self.single_value.type

        elif raw_type is dict:
            self.__map_handle__()

    def __map_handle__(self):
        """

        :return:
        """
        dict_values = set(self.__input_value__.values())
        dict_values_atomic = set([AtomicValue.is_atomic(type(v)) for v in dict_values])

        if len(dict_values) == 0:
            return

        atomic = dict_values_atomic.pop()

        if len(dict_values_atomic) != 0:
            atomic = False

        self.map_value = {}
        for k, v in self.__input_value__.items():
            if atomic:
                self.map_value[str(k)] = AtomicValue(v)

            else:
                self.map_value[str(k)] = AtomicValue(json.dumps(v))

        if atomic:
            v = AtomicValue(dict_values.pop())
            self.type = "map<STRING, {}>".format(v.type.name)
        else:
            self.type = "map<STRING, STRING>"
