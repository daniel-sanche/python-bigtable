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
from typing import TYPE_CHECKING, Any
from .row_response import row_key
from dataclasses import dataclass
from google.cloud.bigtable.row_filters import RowFilter

if TYPE_CHECKING:
    from google.cloud.bigtable import RowKeySamples


@dataclass
class _RangePoint:
    # model class for a point in a row range
    key: row_key
    is_inclusive: bool


class ReadRowsQuery:
    """
    Class to encapsulate details of a read row request
    """

    def __init__(
        self,
        row_keys: list[str | bytes] | str | bytes | None = None,
        limit:int|None=None,
        row_filter:RowFilter|dict[str,Any]|None=None,
    ):
        self.row_keys: set[bytes] = set()
        self.row_ranges: list[tuple[_RangePoint, _RangePoint]] = []
        if row_keys:
            self.add_rows(row_keys)
        self.limit:int|None = limit
        self.filter:RowFilter|dict[str,Any] = row_filter

    def get_limit(self) -> int | None:
        return self._limit

    def set_limit(self, new_limit: int|None):
        if new_limit is not None and new_limit < 0:
            raise ValueError("limit must be >= 0")
        self._limit = new_limit
        return self

    def get_filter(self) -> RowFilter:
        return self._filter

    def set_filter(self, row_filter: RowFilter|dict|None) -> ReadRowsQuery:
        if not (isinstance(row_filter, dict) or isinstance(row_filter, RowFilter) or row_filter is None):
            raise ValueError("row_filter must be a RowFilter or corresponding dict representation")
        self._filter = row_filter
        return self


    limit = property(get_limit, set_limit)
    filter = property(get_filter, set_filter)

    def add_rows(self, row_keys: list[str | bytes] | str | bytes) -> ReadRowsQuery:
        if not isinstance(row_keys, list):
            row_keys = [row_keys]
        update_set = set()
        for k in row_keys:
            if isinstance(k, str):
                k = k.encode()
            elif not isinstance(k, bytes):
                raise ValueError("row_keys must be strings or bytes")
            update_set.add(k)
        self.row_keys.update(update_set)
        return self

    def add_range(
        self,
        start_key: str | bytes | None = None,
        end_key: str | bytes | None = None,
        start_is_inclusive: bool = True,
        end_is_inclusive: bool = False,
    ) -> ReadRowsQuery:
        if isinstance(start_key, str):
            start_key = start_key.encode()
        elif start_key is not None and not isinstance(start_key, bytes):
            raise ValueError("start_key must be a string or bytes")
        if isinstance(end_key, str):
            end_key = end_key.encode()
        elif end_key is not None and not isinstance(end_key, bytes):
            raise ValueError("end_key must be a string or bytes")

        self.row_ranges.append(
            (
                _RangePoint(start_key, start_is_inclusive) if start_key is not None else None,
                _RangePoint(end_key, end_is_inclusive) if end_key is not None else None
            )
        )
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

    def to_dict(self) -> dict[str, Any]:
        """
        Convert this query into a dictionary that can be used to construct a
        ReadRowsRequest protobuf
        """
        ranges = []
        for start, end in self.row_ranges:
            new_range = {}
            if start is not None:
                key = "start_key_closed" if start.is_inclusive else "start_key_open"
                new_range[key] = start.key
            if end is not None:
                key = "end_key_closed" if end.is_inclusive else "end_key_open"
                new_range[key] = end.key
            ranges.append(new_range)
        row_keys = list(self.row_keys)
        row_keys.sort()
        row_set = {"row_keys": row_keys, "row_ranges": ranges}
        final_dict = {
            "rows": row_set,
        }
        dict_filter = self.filter.to_dict() if isinstance(self.filter, RowFilter) else self.filter
        if dict_filter:
            final_dict["filter"] = dict_filter
        if self.limit is not None:
            final_dict["rows_limit"] = self.limit
        return final_dict
