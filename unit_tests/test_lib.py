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

import mock
import unittest
from gluster import lib


class TestResolveToIp(unittest.TestCase):
    def setUp(self):
        pass

    @mock.patch("get_local_ip")
    def test(self):
        # Mock get_local_ip and assert return.
        pass

    def tearDown(self):
        pass


class TestGetLocalHostName(unittest.TestCase):
    def setUp(self):
        pass

    @mock.patch("open")
    def test(self):
        # Mock the /etc/hostnames file and assert return.
        pass

    def tearDown(self):
        pass


class TestTranslateToBytes(unittest.TestCase):
    def setUp(self):
        self.tests = {
            "1TB": 1099511627776.0,
            "8.2KB": 8396.8,
            "2BYTES": 2.0
        }

    def test(self):
        for test, correct in self.tests.items():
            self.assertEqual(lib.translate_to_bytes(test), correct)

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()