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

TEST_ROWS = [
  "row_key_1",
  b"row_key_2",
]

class TestReadRowsQuery(unittest.TestCase):

    @staticmethod
    def _get_target_class():
        from google.cloud.bigtable.read_rows_query import ReadRowsQuery
        return ReadRowsQuery

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    def test_ctor_defaults(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        self.assertEqual(query.row_ranges, [])
        self.assertEqual(query.filter, None)
        self.assertEqual(query.limit, None)

    def test_ctor_explicit(self):
        from google.cloud.bigtable.row_filters import RowFilterChain
        filter_ = RowFilterChain()
        query = self._make_one(["row_key_1", "row_key_2"], limit=10, row_filter=filter_)
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn("row_key_1".encode(), query.row_keys)
        self.assertIn("row_key_2".encode(), query.row_keys)
        self.assertEqual(query.row_ranges, [])
        self.assertEqual(query.filter, filter_)
        self.assertEqual(query.limit, 10)

    def test_ctor_invalid_limit(self):
        with self.assertRaises(ValueError):
            self._make_one(limit=-1)

    def test_set_filter(self):
        from google.cloud.bigtable.row_filters import RowFilterChain
        filter1 = RowFilterChain()
        query = self._make_one()
        self.assertEqual(query.filter, None)
        result = query.set_filter(filter1)
        self.assertEqual(query.filter, filter1)
        self.assertEqual(result, query)
        filter2 = RowFilterChain()
        result = query.set_filter(filter2)
        self.assertEqual(query.filter, filter2)
        self.assertEqual(query.filter, filter2)
        result = query.set_filter(None)
        self.assertEqual(query.filter, None)
        self.assertEqual(result, query)

    def test_set_limit(self):
        query = self._make_one()
        self.assertEqual(query.limit, None)
        result = query.set_limit(10)
        self.assertEqual(query.limit, 10)
        self.assertEqual(result, query)
        result = query.set_limit(0)
        self.assertEqual(query.limit, 0)
        self.assertEqual(result, query)
        with self.assertRaises(ValueError):
            query.set_limit(-1)

    def test_add_rows_str(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_str = "test_row"
        result = query.add_rows(input_str)
        self.assertEqual(len(query.row_keys), 1)
        self.assertIn(input_str.encode(), query.row_keys)
        self.assertEqual(result, query)
        input_str2 = "test_row2"
        result = query.add_rows(input_str2)
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(input_str.encode(), query.row_keys)
        self.assertIn(input_str2.encode(), query.row_keys)
        self.assertEqual(result, query)

    def test_add_rows_bytes(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_bytes = b"test_row"
        result = query.add_rows(input_bytes)
        self.assertEqual(len(query.row_keys), 1)
        self.assertIn(input_bytes, query.row_keys)
        self.assertEqual(result, query)
        input_bytes2 = b"test_row2"
        result = query.add_rows(input_bytes2)
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(input_bytes, query.row_keys)
        self.assertIn(input_bytes2, query.row_keys)
        self.assertEqual(result, query)

    def test_add_rows_batch(self):
        query = self._make_one()
        self.assertEqual(query.row_keys, set())
        input_batch = ["test_row", b"test_row2", "test_row3"]
        result = query.add_rows(input_batch)
        self.assertEqual(len(query.row_keys), 3)
        self.assertIn(b"test_row", query.row_keys)
        self.assertIn(b"test_row2", query.row_keys)
        self.assertIn(b"test_row3", query.row_keys)
        self.assertEqual(result, query)
        # test adding another batch
        query.add_rows(["test_row4", b"test_row5"])
        self.assertEqual(len(query.row_keys), 5)
        self.assertIn(input_batch[0].encode(), query.row_keys)
        self.assertIn(input_batch[1], query.row_keys)
        self.assertIn(input_batch[2].encode(), query.row_keys)
        self.assertIn(b"test_row4", query.row_keys)
        self.assertIn(b"test_row5", query.row_keys)

    def test_add_rows_invalid(self):
        query = self._make_one()
        with self.assertRaises(ValueError):
            query.add_rows(1)
        with self.assertRaises(ValueError):
            query.add_rows(["s", 0])

    def test_duplicate_rows(self):
        # should only hold one of each input key
        key_1 = b"test_row"
        key_2 = b"test_row2"
        query = self._make_one(row_keys=[key_1, key_1, key_2])
        self.assertEqual(len(query.row_keys), 2)
        self.assertIn(key_1, query.row_keys)
        self.assertIn(key_2, query.row_keys)
        key_3 = "test_row3"
        query.add_rows([key_3 for _ in range(10)])
        self.assertEqual(len(query.row_keys), 3)

    def test_add_range(self):
        pass

    def test_to_dict(self):
        pass

    def test_shard(self):
        pass
    
