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

from collections import OrderedDict
from typing import Sequence
from functools import total_ordering

# Type aliases used internally for readability.
row_key = bytes
family_id = str
qualifier = bytes
row_value = bytes


class RowResponse(Sequence["CellResponse"]):
    """
    Model class for row data returned from server

    Does not represent all data contained in the row, only data returned by a
    query.
    Expected to be read-only to users, and written by backend

    Can be indexed:
    cells = row["family", "qualifier"]
    """

    def __init__(self, key: row_key, cells: list[CellResponse]):
        """Expected to be used internally only"""
        self.row_key = key
        self.cells: OrderedDict[
            family_id, OrderedDict[qualifier, list[CellResponse]]
        ] = OrderedDict()

    def get_cells(
        self, family: str | None, qualifer: str | bytes | None
    ) -> list[CellResponse]:
        """
        Returns cells sorted in Bigtable native order:
            - Family lexicographically ascending
            - Qualifier lexicographically ascending
            - Timestamp in reverse chronological order

        If family or qualifier not passed, will include all

        Syntactic sugar: cells = row["family", "qualifier"]
        """
        if family is None and qualifier is not None:
            raise ValueError("Qualifier passed without family")
        raise NotImplementedError

    def get_index(self) -> dict[family_id, list[qualifier]]:
        """
        Returns a list of family and qualifiers for the object
        """
        raise NotImplementedError

    def __str__(self):
        """
        Human-readable string representation

        (family, qualifier)   cells
        (ABC, XYZ)            [b"123", b"456" ...(+5)]
        (DEF, XYZ)            [b"123"]
        (GHI, XYZ)            [b"123", b"456" ...(+2)]
        """
        raise NotImplementedError

@total_ordering
class CellResponse:
    """
    Model class for cell data

    Does not represent all data contained in the cell, only data returned by a
    query.
    Expected to be read-only to users, and written by backend
    """

    def __init__(
        self,
        value: row_value,
        row: row_key,
        family: family_id,
        column_qualifier: qualifier,
        timestamp: int,
        labels: list[str] | None = None,
    ):
        """Expected to be used internally only"""
        self.value = value
        self.row_key = row
        self.family = family
        self.column_qualifier = column_qualifier
        # keep a utf-8 encoded string for lexical comparison
        self._column_qualifier_str = column_qualifier.decode("UTF-8")
        self.timestamp = timestamp
        self.labels = labels if labels is not None else []

    def decode_value(self, encoding="UTF-8", errors=None) -> str:
        """decode bytes to string"""
        return self.value.decode(encoding, errors)

    def __int__(self) -> int:
        """
        Allows casting cell to int
        Interprets value as a 64-bit big-endian signed integer, as expected by
        ReadModifyWrite increment rule
        """
        return int.from_bytes(self.value, byteorder="big", signed=True)

    def __str__(self) -> str:
        """
        Allows casting cell to str
        Prints encoded byte string, same as printing value directly.
        """
        return str(self.value)

    """For Bigtable native ordering"""

    def __lt__(self, other) -> bool:
        if not isinstance(other, CellResponse):
            return NotImplemented
        return (self.family, self._column_qualifier_str, self.timestamp) < \
            (other.family, other._column_qualifier_str, other.timestamp)

    def __eq__(self, other) -> bool:
        if not isinstance(other, CellResponse):
            return NotImplemented
        return self.row_key == other.row_key and \
                self.family == other.family and \
                self.column_qualifier == other.column_qualifier and \
                self.value == other.value and \
                self.timestamp == other.timestamp and \
                len(self.labels) == len(other.labels) and \
                all([l in other.labels for l in self.labels])
