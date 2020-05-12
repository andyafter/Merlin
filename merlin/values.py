#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import json
from decimal import Decimal
from enum import Enum


class AtomicValueException(Exception):

    def __init__(self):
        pass


class SerializedTypes(Enum):
    JSON = 1


class AtomicTypes(Enum):
    LONG = 1
    DOUBLE = 2
    BOOL = 3
    STRING = 4


class CompositeTypes(Enum):
    LIST = 1
    MAP = 2


class AtomicValue:
    """
    An atomic value meaning a value that natively represented such as
    int float bool and string
    """

    ACCEPTED_PYTHON_TYPES = set([int, float, bool, str])
    MAPPED_TYPES = {int: ('long', AtomicTypes.LONG),
                    float: ('double', AtomicTypes.DOUBLE),
                    bool: ('bool', AtomicTypes.BOOL),
                    str: ('string', AtomicTypes.STRING)
                    }

    __slots__ = ['long', 'double', 'bool', 'string', 'type', '__raw_type__', '__attr_id__']

    def __init__(self, value):
        self.__raw_type__ = type(value)

        if self.is_atomic(self.__raw_type__) is False:
            raise AtomicValueException()

        self.__set_value__(value)

    def __set_value__(self, attr_value):
        self.__attr_id__, self.type = self.MAPPED_TYPES[self.__raw_type__]
        self.__setattr__(self.__attr_id__, attr_value)

    def asdict(self):

        d = {'type': self.type.name.lower()}

        for kv in self.MAPPED_TYPES.values():
            if kv[0] == self.__attr_id__:
                d[kv[0]] = self.__getattribute__(self.__attr_id__)
            else:
                d[kv[0]] = None

        return d

    @staticmethod
    def is_atomic(raw_type):
        return raw_type in AtomicValue.ACCEPTED_PYTHON_TYPES


class StructuredValue:
    """
    An output value which can be atomic or composite
    """
    __slots__ = ['atomic_value', 'list_value', 'map_value', 'type', '__input_value__', 'value_id',
                 'composite_type']

    def __init__(self, input_value, value_id, serialize_type=None, serializer=None):
        """

        :param input_value:
        :param value_id:
        :param serialize: bool Do we need to serialize
        :param serialize_type: SerializedTypes
        :param serializer: instance of a serializer e.g json.dumps
        """
        self.__input_value__ = input_value
        self.value_id = value_id

        need_serialize = serialize_type is not None

        if need_serialize:
            self.__input_value__ = serializer(self.__input_value__)

        raw_type = type(self.__input_value__)

        if AtomicValue.is_atomic(raw_type):
            self.composite_type = None
            self.atomic_value = AtomicValue(self.__input_value__)
            if need_serialize:
                self.type = serialize_type.name.lower()

            else:
                self.type = self.atomic_value.type.name.lower()


        elif raw_type is dict:
            self.__map_handle__()
            self.composite_type = CompositeTypes.MAP

        elif raw_type is list:
            self.__list_handle__()
            self.composite_type = CompositeTypes.LIST

        elif raw_type is Decimal:
            self.__handle_decimal__()
            self.composite_type = None

    def __map_handle__(self):
        """
        handle a map in input
        :return:
        """
        dict_values = set(self.__input_value__.values())
        dict_values_atomic = set([AtomicValue.is_atomic(type(v)) for v in dict_values])

        if len(dict_values) == 0:
            return

        atomic = dict_values_atomic.pop()

        if len(dict_values_atomic) != 0:
            atomic = False

        if atomic:
            self.map_value = self.__input_value__

        else:
            for k, v in self.__input_value__:
                self.map_value[str(k)] = json.dumps(v)

        if atomic:
            v = AtomicValue(dict_values.pop())
            self.type = "map_{}".format(v.type.name.lower())
        else:
            self.type = "map_string"

    def __list_handle__(self):

        input_list = self.__input_value__

        if len(input_list) == 0:
            self.type = "vec_string"
            return

        set_types = set([type(v) for v in input_list])
        set_atomic = set(AtomicValue.is_atomic(type(v)) for v in input_list)

        atomic = False
        if len(set_types) == 1 and len(set_atomic) == 1:
            atomic = set_atomic.pop()

        if atomic:
            v = AtomicValue(input_list[0])
            self.type = "vec_{}".format(v.type.name.lower())
            self.list_value = input_list
        else:
            self.type = "vec_string"
            self.list_value = [json.dumps(v) for v in input_list]

    def __handle_decimal__(self):
        decimal = self.__input_value__
        if Decimal(decimal).remainder_near(1).is_zero():
            self.atomic_value = AtomicValue(int(decimal))
        else:
            self.atomic_value = AtomicValue(float(decimal))

        self.type = self.atomic_value.type.name.lower()

    def asdict(self):

        keys = ['type']
        keys.extend([x.name.lower() for x in AtomicTypes])
        keys.extend(["{}_{}".format(c, t.name.lower()) for c in ["vec", "map"] for t in AtomicTypes])

        d = {k: None for k in keys}

        if self.composite_type is None:
            for k, v in self.atomic_value.asdict().items():
                d[k] = v

        elif self.composite_type == CompositeTypes.LIST:
            d[self.type] = self.list_value

        elif self.composite_type == CompositeTypes.MAP:
            d[self.type] = self.map_value

        d['type'] = self.type

        return d
