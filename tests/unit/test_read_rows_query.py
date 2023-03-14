# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mock
import pytest
import unittest

import time

class TestReadRowsQuery(unittest.TestCase):

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        return ReadRowsQuery

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        pass


    def test_set_filter(self):
        pass

    def test_add_rows(self):
        pass

    def test_add_range(self):
        pass

    def test_to_dict(self):
        pass
    
