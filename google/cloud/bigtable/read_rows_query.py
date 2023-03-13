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
#
from __future__ import annotations
from typing import TYPE_CHECKING
from collections import namedtuple

if TYPE_CHECKING:
    from google.cloud.bigtable.row_filters import RowFilter
    from google.cloud.bigtable import RowKeySamples

RangePoint = namedtuple("RangePoint", ["key", "is_inclusive"])

class ReadRowsQuery:
    """
    Class to encapsulate details of a read row request
    """

    def __init__(
        self, row_keys: list[str | bytes] | str | bytes | None = None, limit=None, row_filter=None
    ):
        self.row_keys = []
        self.row_ranges = []
        if row_keys:
            self.add_rows(row_keys)

        self.limit = limit
        self.filter = row_filter

    def set_limit(self, limit: int) -> ReadRowsQuery:
        self.limit = limit
        return self

    def set_filter(self, row_filter: "RowFilter") -> ReadRowsQuery:
        self.filter = row_filter
        return self

    def add_rows(self, row_keys: list[str|bytes]|str|bytes) -> ReadRowsQuery:
        if isinstance(row_keys, str) or isinstance(row_keys, bytes):
            row_keys = [row_keys]
        self.row_keys.extend(row_keys)
        return self

    def add_range(
        self, start_key: str | bytes | None = None, end_key: str | bytes | None = None,
        start_is_inclusive: bool = True, end_is_inclusive: bool = False
    ) -> ReadRowsQuery:
        if start_key is None and end_key is None:
            raise ValueError("start_key and end_key cannot both be None")

        self.row_ranges.append((RangePoint(start_key, start_is_inclusive), RangePoint(end_key, end_is_inclusive)))
        return self

    def shard(self, shard_keys: "RowKeySamples" | None = None) -> list[ReadRowsQuery]:
        """
        Split this query into multiple queries that can be evenly distributed
        across nodes and be run in parallel

        Returns:
            - a list of queries that represent a sharded version of the original
              query (if possible)
        """
        raise NotImplementedError

    def to_dict(self) -> dict:
        """
        Convert this query into a dictionary that can be used to construct a
        ReadRowsRequest protobuf
        """
        ranges = []
        for start, end in self.row_ranges:
            new_range = {}
            if start.key is not None:
                key = "start_key_closed" if start.is_inclusive else "start_key_open"
                new_range[key] = start.key
            if end.key is not None:
                key = "end_key_closed" if end.is_inclusive else "end_key_open"
                new_range[key] = end.key
            ranges.append(new_range)
        row_set = {"row_keys": self.row_keys, "row_ranges": ranges}
        final_dict = {"rows": row_set, "filter": self.filter.to_dict(), "limit": self.limit}
        return final_dict
