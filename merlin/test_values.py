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
import unittest

from values import AtomicValue, ComposedValue, SerializedTypes


class AtomicValueTest(unittest.TestCase):

    def test_atomic(self):
        for p in [(10, "long"), (10.0, "double"), (False, "bool"), ("10.0", "string")]:
            v = p[0]
            atomic_attribute = p[1]
            a = AtomicValue(v)

            self.assertEqual(a.type, AtomicValue.MAPPED_TYPES.get(type(v))[1])
            self.assertEqual(a.__getattribute__(atomic_attribute), v)
            self.assertEqual(type(a.asdict()), dict)
            print(a.asdict())

    def test_output_value_list(self):
        o = ComposedValue([10, 12, 13], "test_int_list")
        self.assertEqual(o.type, "vec_long")
        print(o.as_dict())
        o = ComposedValue({'10': 10}, "test_int_map")
        print(o.as_dict())
        o = ComposedValue({'a': 1, 'b': [1, 2, 3], 'c': False}, "json_test",
                          serialize=True, serialize_type=SerializedTypes.JSON, serializer=json.dumps)
        print(o.as_dict())

    # TODO add test for other types


if __name__ == '__main__':
    unittest.main()
