# Copyright 2017 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from gluster import volume
from ipaddress import ip_address
import mock
from result import Ok
import uuid
import unittest


class Test(unittest.TestCase):
    def testParseVolumeList(self):
        with open('unit_tests/vol_list.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            result_list = volume.parse_volume_list("".join(lines))
            self.assertTrue(result_list.is_ok())
            self.assertEqual(1, len(result_list.value), "Expected 1 volume")
            self.assertEqual("chris", result_list.value[0],
                             "Expected 1 volume called chris")

    def testParseVolumeInfo(self):
        with open('unit_tests/vol_info.xml', 'r') as xml_output:
            lines = xml_output.readlines()
            results = volume.parse_volume_info("".join(lines))
            self.assertTrue(results.is_ok())
            for vol in results.value:
                print("parse volume info: {}".format(vol))
                for brick in vol.bricks:
                    print("brick: {}".format(brick))


if __name__ == "__main__":
    unittest.main()
