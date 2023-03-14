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

TEST_VALUE = b'1234'
TEST_ROW_KEY = b'row'
TEST_FAMILY_ID = 'cf1'
TEST_QUALIFIER = b'col'
TEST_TIMESTAMP = time.time_ns()
TEST_LABELS = ['label1', 'label2']

class TestRowResponse(unittest.TestCase):

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row_response import RowResponse
        return RowResponse

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_get_cells(self):
        pass

    def test__get_all_from_family(self):
        pass

    def test___str__(self):
        pass

    def test__repr__(self):
        pass

    def test_to_dict(self):
        pass

    def test_iteration(self):
        # should be able to iterate over the RowResponse as a list
        pass

    def test_contains_cell(self):
        pass

    def test_contains_family_id(self):
        pass

    def test_contains_family_qualifier_tuple(self):
        pass

    def test___len__(self):
        pass

    def test_int_indexing(self):
        # should be able to index into underlying list with an index number directly
        pass

    def test_slice_indexing(self):
        # should be able to index with a range of indices
        pass

    def test_family_indexing(self):
        # should be able to retrieve cells in a family
        pass

    def test_family_qualifier_indexing(self):
        # should be able to retrieve cells in a family/qualifier tuplw
        pass

    def test_keys(self):
        # should be able to retrieve (family,qualifier) tuples as keys
        pass

    def test_values(self):
        # values should return the list of all cells
        pass

    def test_index_of(self):
        # given a cell, should find index in underlying list
        pass

    def test_count(self):
        pass


class TestCellResponse(unittest.TestCase):

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.row_response import CellResponse
        return CellResponse

    def _make_one(self, *args, **kwargs):
        if len(args) == 0:
            args = (TEST_VALUE, TEST_ROW_KEY, TEST_FAMILY_ID, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        return self._get_target_class()(*args, **kwargs)

    def test_ctor(self):
        cell = self._make_one(TEST_VALUE, TEST_ROW_KEY, TEST_FAMILY_ID, TEST_QUALIFIER, TEST_TIMESTAMP, TEST_LABELS)
        self.assertEqual(cell.value, TEST_VALUE)
        self.assertEqual(cell.row_key, TEST_ROW_KEY)
        self.assertEqual(cell.family, TEST_FAMILY_ID)
        self.assertEqual(cell.column_qualifier, TEST_QUALIFIER)
        self.assertEqual(cell.timestamp_ns, TEST_TIMESTAMP)
        self.assertEqual(cell.labels, TEST_LABELS)

    def test_to_dict(self):
        pass

    def test_int_value(self):
        pass

    def test_int_value_string_formatting(self):
        pass

    def test___str__(self):
        pass

    def test___repr__(self):
        pass

    def test_print(self):
        pass

    def test_equality(self):
        pass

    def test_hash(self):
        pass

    def test_ordering(self):
        pass

    


